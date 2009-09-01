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

my @chunkbuffer;
my $backend    = 0;
my $linenumber = 0;
my $statementbuf = '';
my $timestamp = '';
my $duration = 0;


my $LINENUMBER = 0;
my $STATEMENT = 1;

my $linecounter = 0;

my $eins = 1;

while ( my $line = <ARGV> ) {
    $linecounter++;
    next if $line =~ /^\s+$/;

#    print "$linecounter\n";

    if ( $line =~ m/$pglogchunkre/ ) {

        my ( $curbackendpid, $curlinenumber, $foobar, $content ) = ( $1, $2, $3, $4 );

        print "$curbackendpid, $curlinenumber, $foobar\n";

# If this is the first chunk of a log line we might need to flush a
# previously stored log line

        my $bla = $foobar;
        print "Cur Chunk Number: $bla\n";
        print Dumper $bla;
        if ( $bla == 1 ) {

            if ($chunkbuffer[$curbackendpid]->[$STATEMENT]) {
                logme($timestamp, $duration, $curbackendpid);
                $chunkbuffer[$curbackendpid]->[$STATEMENT] = '';
            }

# Extract timestamp, duration and log statement

            if ( $content =~ m/$pglogdurationre/ ) {
                my ( $statementpart );
                ( $timestamp, $duration, $statementpart ) = ( $1, $2, $3 );


# If this statement is of interest, save the statement and log number since
# there might be other chunks belonging to this statement

                if ( $duration > $threshold ) {
                    $chunkbuffer[$curbackendpid]->[$LINENUMBER] = $curlinenumber;
                    $chunkbuffer[$curbackendpid]->[$STATEMENT] = $statementpart;
                }
            }
            else {
                croak "This doesn't look like something I expect in a statement log line";
            }
        }


# This logchunk belongs to a log line we want to log

        elsif ( $chunkbuffer[$curbackendpid]->[$LINENUMBER] == $curlinenumber ) {
            $chunkbuffer[$curbackendpid]->[$STATEMENT] .= $content;
        }
    }
    else {
        croak "This doesn't look like something I expect in a statement log\nStatement\n\n$line";
    }
}


# FIXME: Flush chunkbuffer
#if ($statementbuf) {
#    logme($timestamp, $duration, $statementbuf);
#}


sub logme {
    my ($timestamp, $duration, $curbackendpid) = @_;

    my $statement = $chunkbuffer[$curbackendpid]->[$STATEMENT];
    $statement =~ s/\s+/ /g;

    print "$timestamp: $duration ms: $statement\n";
}
