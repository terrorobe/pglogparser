#!/usr/bin/perl

use strict;
use warnings;

my $pglogchunkre = qr!\[(\d+)-(\d+)\]\s+(.*)!x;

my $pglogdurationre = qr!
                .*?                     # Timestamp
                LOG:\s+                 # Log identifier
                duration:\s+([\d.]+)\s+ms # Diratuion
                !x;

my $threshold = 10;

my $backend    = 0;
my $linenumber = 0;
my $printme    = 0;

while ( my $line = <STDIN> ) {

    if ( $line =~ m/$pglogchunkre/ ) {

        my ( $curbackend, $curlinenumber, $content ) = ( $1, $2, $3 );

        if ( $curlinenumber == 1 ) {
            if ( $content =~ m/$pglogdurationre/ ) {
                if ( $1 > $threshold ) {
                    $backend = $curbackend;
                    $printme = 1;
                }
            }
        }
        elsif ( $curbackend == $backend ) {
            $printme = 1;
        }
    }
    if ($printme) {
        print $line;
    }
    $printme = 0;
}
