#!/usr/bin/perl

use strict;
use warnings;

my $pglogchunkre = qr!\[(\d+)-(\d+)\]\s+(.*)!x;

my $pglogdurationre = qr!
                .*?                     # Timestamp
                LOG:\s+                 # Log identifier
                duration:\s+([\d.]+)\s+ms # Duration
                !x;

my $threshold = 1000; # msec

my $backend    = 0;
my $linenumber = 0;
my $printme    = 0;

while ( my $line = <ARGV> ) {

    if ( $line =~ m/$pglogchunkre/ ) {

        my ( $curbackend, $curlinenumber, $content ) = ( $1, $2, $3 );

# If this is the first line number of a chunk and it contains a duration
# above the threshold then log it

        if ( $curlinenumber == 1 ) {
            if ( $content =~ m/$pglogdurationre/ ) {
                if ( $1 > $threshold ) {
                    $backend = $curbackend;
                    $printme = 1;
                }
            }
        }

# If the backend number hasn't been changed, this needs to get printed
        elsif ( $curbackend == $backend ) {
            $printme = 1;
        }

# If it's neither a starting or a continuous line, reset backend for good
        else {
            $backend = -1;
        }
    }

    if ($printme) {
        print $line;
    }
    $printme = 0;
}
