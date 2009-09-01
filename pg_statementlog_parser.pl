#!/usr/bin/perl

use strict;
use warnings;

$|++;

use Carp;
use Data::Dumper;

my $pglogchunkre = qr!
                \[(\d+)]:\s+              # Backend PID
                \[(\d+)-(\d+)\]\s       # Logline and Chunk Counter
                (.*)!x;                 # Log Message

my $pglogdurationre = qr!
                (.*?)\s+                # Timestamp
                LOG:\s+                 # Log identifier
                duration:\s+([\d.]+)\s+ms\s+ # Duration
                statement:\s+(.*)       # Statement
                !x;

my $threshold = 300000; # msec
my $debug = 0;

my @chunkbuffer;


my $LINENUMBER = 0;
my $STATEMENT = 1;
my $TIMESTAMP = 2;
my $DURATION = 3;

my $linecounter = 0;


while ( my $line = <ARGV> ) {
    $linecounter++;
    next if $line =~ /^\s+$/;

    if ( $line =~ m/$pglogchunkre/ ) {

        my ( $curbackendpid, $curlinenumber, $curchunknumber, $content ) = ( $1, $2, $3, $4 );


# If this is the first chunk of a log line we might need to flush a
# previously stored log line

        if ( $curchunknumber == 1 ) {

            if ($chunkbuffer[$curbackendpid]->[$STATEMENT]) {
                logme($curbackendpid);
                $chunkbuffer[$curbackendpid]->[$STATEMENT] = '';
            }

# Extract timestamp, duration and log statement

            if ( $content =~ m/$pglogdurationre/ ) {
                my ( $timestamp, $duration, $statementpart ) = ( $1, $2, $3 );


# If this statement is of interest, save the statement and log number since
# there might be other chunks belonging to this statement

                if ( $duration > $threshold ) {
                    $chunkbuffer[$curbackendpid]->[$LINENUMBER] = $curlinenumber;
                    $chunkbuffer[$curbackendpid]->[$STATEMENT] = $statementpart;
                    $chunkbuffer[$curbackendpid]->[$TIMESTAMP] = $timestamp;
                    $chunkbuffer[$curbackendpid]->[$DURATION] = $duration;
                }
            }
            else {
                if ($debug) {
                    carp "Skipping unknown entry:\n\n$content";
                }
            }
        }


# This logchunk belongs to a log line we want to log

        elsif ( $chunkbuffer[$curbackendpid]->[$LINENUMBER] && $chunkbuffer[$curbackendpid]->[$LINENUMBER] == $curlinenumber ) {
            $chunkbuffer[$curbackendpid]->[$STATEMENT] .= $content;
        }
    }
    else {
        croak "This doesn't look like something I expect in a statement log\nLine\n\n$line";
    }
}


# FIXME: Flush chunkbuffer
#if ($statementbuf) {
#    logme($timestamp, $duration, $statementbuf);
#}


sub logme {
    my ($curbackendpid) = @_;

    my $statement = $chunkbuffer[$curbackendpid]->[$STATEMENT];
    my $timestamp = $chunkbuffer[$curbackendpid]->[$TIMESTAMP];
    my $duration = $chunkbuffer[$curbackendpid]->[$DURATION];

    $statement =~ s/\s+/ /g;

    print "$timestamp: $duration ms: $statement\n";
}
