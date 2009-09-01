#!/usr/bin/perl

use strict;
use warnings;

use Carp;

use constant {
    LINENUMBER => 0,
    STATEMENT  => 1,
    TIMESTAMP  => 2,
    DURATION   => 3,
};

my $pglogchunkre = qr!
                \[(\d+)]:\s+              # Backend PID
                \[(\d+)-(\d+)\]\s       # Logline and Chunk Counter
                (.*)!x;    # Log Message

my $pglogdurationre = qr!
                (.*?)\s+                # Timestamp
                LOG:\s+                 # Log identifier
                duration:\s+([\d.]+)\s+ms\s+ # Duration
                statement:\s+(.*)       # Statement
                !x;

my $threshold = 300 * 10**3;    # msec
my $debug     = 1;

my @chunkbuffer;

while ( my $line = <ARGV> ) {

    next if $line =~ /^\s+$/;

    # Does he look like a PostreSQL log line?!

    if ( $line =~ m/$pglogchunkre/ ) {

        my ( $curbackendpid, $curlinenumber, $curchunknumber, $content )
            = ( $1, $2, $3, $4 );

        if ( $curchunknumber == 1 ) {

            # If this is the first chunk of a log line we might need to flush a
            # previously stored log line

            if ( $chunkbuffer[$curbackendpid]->[STATEMENT] ) {
                logme($curbackendpid);
                $chunkbuffer[$curbackendpid]->[STATEMENT] = '';
            }

            # Does he look like a statement log entry?!
            # Extract timestamp, duration and log statement

            if ( $content =~ m/$pglogdurationre/ ) {
                my ( $timestamp, $duration, $statementpart ) = ( $1, $2, $3 );

              # If this statement is of interest, save it's data for now since
              # there might be other chunks belonging to this statement

                if ( $duration > $threshold ) {
                    $chunkbuffer[$curbackendpid]->[LINENUMBER]
                        = $curlinenumber;
                    $chunkbuffer[$curbackendpid]->[STATEMENT]
                        = $statementpart;
                    $chunkbuffer[$curbackendpid]->[TIMESTAMP] = $timestamp;
                    $chunkbuffer[$curbackendpid]->[DURATION]  = $duration;
                }
            }
            else {
                if ($debug) {
                    carp "Skipping unknown entry:\n$content";
                }
            }
        }

        # So we're not the first line of a chunk...
        # Does this logchunk belong to a log line we want to log?

        elsif ($chunkbuffer[$curbackendpid]->[LINENUMBER]
            && $chunkbuffer[$curbackendpid]->[LINENUMBER] == $curlinenumber )
        {
            $chunkbuffer[$curbackendpid]->[STATEMENT] .= $content;
        }
    }
    else {
        croak
            "This doesn't look like something I expect in a statement log\nLine\n\n$line";
    }
}

# Flush any leftover statements from the chunk buffer
for my $backendpid ( 0 .. @chunkbuffer ) {
    if ( $chunkbuffer[$backendpid]->[STATEMENT] ) {
        logme($backendpid);
    }
}

sub logme {
    my ($curbackendpid) = @_;

    my $statement = $chunkbuffer[$curbackendpid]->[STATEMENT];
    my $timestamp = $chunkbuffer[$curbackendpid]->[TIMESTAMP];
    my $duration  = $chunkbuffer[$curbackendpid]->[DURATION];

    $statement =~ s/\s+/ /g;

    print "$timestamp: $duration ms: $statement\n";
}
