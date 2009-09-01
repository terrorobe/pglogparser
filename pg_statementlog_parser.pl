#!/usr/bin/perl

use strict;
use warnings;

my $pglogchunkre = qr!\[(\d+)-(\d+)\]\s(.*)!x;

my $pglogdurationre = qr!
                (.*?)\s+                # Timestamp
                LOG:\s+                 # Log identifier
                duration:\s+([\d.]+)\s+ms\s+ # Duration
                statement:\s+(.*)       # Statement
                !x;

my $threshold = 1000; # msec

my $backend    = 0;
my $linenumber = 0;
my $statementbuf = '';
my $timestamp = '';
my $duration = 0;

while ( my $line = <ARGV> ) {

    if ( $line =~ m/$pglogchunkre/ ) {

        my ( $curbackend, $curlinenumber, $content ) = ( $1, $2, $3, );

# If this is the first line number of a chunk and it contains a duration
# above the threshold then log it

        if ( $curlinenumber == 1 ) {

            if ($statementbuf) {
                print "$timestamp: $duration ms: $statementbuf\n";
                $statementbuf = '';
            }

            if ( $content =~ m/$pglogdurationre/ ) {
                my ( $statementpart );
                ( $timestamp, $duration, $statementpart ) = ( $1, $2, $3 );

                if ( $duration > $threshold ) {
                    $backend = $curbackend;
                    $statementbuf = $statementpart;
                }
            }
        }

# If the backend number hasn't been changed, this needs to get printed
        elsif ( $curbackend == $backend ) {
            $statementbuf .= $content;
        }

# If it's neither a starting or a continuous line, reset backend for good
        else {
            $backend = -1;
        }
    }
}


if ($statementbuf) {
    print "$timestamp: $duration ms: $statementbuf\n";
}
