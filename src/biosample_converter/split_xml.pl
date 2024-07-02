#!/usr/bin/perl

use strict;
use warnings;
use File::Basename;

# Check arguments
if (@ARGV < 1) {
    die "Usage: $0 input.xml [output_dir]\n";
}

# Input file and output directory
my $input_file = $ARGV[0];
my $output_dir = $ARGV[1] // '.';
my $max_recs = 30000;
my $head = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<BioSampleSet>\n";
my $tail = "\n</BioSampleSet>";

# Ensure output directory exists
unless (-d $output_dir) {
    die "Output directory $output_dir does not exist.\n";
}

# Open input file
open my $in, '<', $input_file or die "Cannot open input file $input_file: $!\n";

my $file_nr = 0;
my $rec_count = 0;
my $out;
my $buffer = '';

while (my $line = <$in>) {
    $buffer .= $line;

    if ($line =~ m{</BioSample>}) {
        $rec_count++;
        
        if ($rec_count % $max_recs == 1) {
            if ($out) {
                print $out $tail;
                close $out;
            }
            my $output_file = sprintf("%s/split_bs%d.xml", $output_dir, ++$file_nr);
            open $out, '>', $output_file or die "Cannot open output file $output_file: $!\n";
            if ($rec_count == 1) {
                # $head
            }else{
                print $out $head;
            }
        }

        print $out $buffer;
        $buffer = '';
    }
}

if ($out) {
    print $out $tail;
    close $out;
}

close $in;
