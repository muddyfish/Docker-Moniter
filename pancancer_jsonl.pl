#!/usr/bin/perl

use strict;
use warnings FATAL => 'all';
use File::Temp qw(tempdir);
use IO::Uncompress::Gunzip qw($GunzipError);
use Data::Dumper;
use JSON qw(decode_json);
use Const::Fast qw(const);
use File::Fetch;
use File::Copy qw(move);
use autodie;
use List::Util qw(first);

const my $DEFAULT_URL => 'http://pancancer.info/gnos_metadata/latest';

my %execute = ( 'by_size' => \&donors_by_size,
                'check_analysis' => \&check_analysis,
                'dump_bas' => \&bas_for_donor,
                'genotype' => \&genotype_chk,
                'header_idclash' => \&header_idclash);

my $mode = shift @ARGV;
unless($mode) {
  warn "USAGE: Please provide a valid mode, these are:\n";
  for(sort keys %execute) {
    warn "\t$_\n";
  }
  exit 1;
}

my $tmpdir = tempdir( CLEANUP => 1 );
my $input = manifest($tmpdir);
my $z = new IO::Uncompress::Gunzip $input or die "gunzip failed: $GunzipError\n";
$execute{$mode}->($z, @ARGV);
close $z;

sub genotype_chk {
  my ($input, $cutoff, $restrict_donors) = @_;
  my @donor_lst;
  @donor_lst = load_donors($restrict_donors) if(defined $restrict_donors);
  $cutoff ||= 0.90;
  my $missing = 0;
  my $results = 0;
  my $bad = 0;
  my @gt_header = qw( donor_unique_id
                      tumour_aliquot_id
                      frac_matched_genotype
                      frac_informative_genotype
                      contamination
                      avg_depth
                      AscatNormalContamination
                      solution_possible
                      goodnessOfFit);
  print join("\t", @gt_header),"\n";

  while(my $jsonl = <$z>) {
    my $donor = decode_json($jsonl);
    next if($donor->{flags}->{is_donor_blacklisted});
    next if($donor->{flags}->{is_test});
    next if($donor->{flags}->{is_manual_qc_failed});
    next unless($donor->{flags}->{is_sanger_variant_calling_performed});
    next unless( first {$_ eq $donor->{donor_unique_id} } @donor_lst);
#    next unless( first {$_ eq 'sanger'} @{$donor->{flags}->{variant_calling_performed}});
    $results++;
    my $qc = $donor->{variant_calling_results}->{sanger_variant_calling}->{workflow_details}->{variant_qc_metrics};
    for my $tum_aliq(@{$donor->{aligned_tumor_specimen_aliquots}}) {
      my $gt = $qc->{$tum_aliq}->{genotype};
      unless(exists $gt->{frac_matched_genotype}) {
        $missing++;
        next;
      }
      if($gt->{frac_matched_genotype} < $cutoff) {
        #dig into contamination
        print sprintf "%s\t%s\t%.2f\t%.2f\t%.5f\t%.2f\t%.5f\t%s\t%.2f\n",
          $donor->{donor_unique_id},
          $tum_aliq,
          $gt->{frac_matched_genotype},
          $gt->{frac_informative_genotype},
          $qc->{$tum_aliq}->{contamination}->{$tum_aliq}->{contamination},
          $qc->{$tum_aliq}->{contamination}->{$tum_aliq}->{avg_depth},
          $qc->{$tum_aliq}->{cnv}->{NormalContamination},
          $qc->{$tum_aliq}->{cnv}->{solution_possible},
          $qc->{$tum_aliq}->{cnv}->{goodnessOfFit},
          ;
        $bad++;
      }
    }


    #print $jsonl,"\n";
#    exit;
  }
  warn "Sanger result sets: $results\n";
  warn "Results without genotype: $missing\n";
  warn sprintf "Results below %.2f: %d\n", $cutoff, $bad;
}

sub bas_for_donor {
  my ($input, @uniq_donor_ids) = @_;
  while(my $jsonl = <$z>) {
    my $donor = decode_json($jsonl);
    next unless( first {$_ eq $donor->{donor_unique_id}} @uniq_donor_ids);
    print 'DONOR: ',$donor->{donor_unique_id},"\n";
    if(exists $donor->{aligned_tumor_specimens}) {
      for my $tumour(@{$donor->{aligned_tumor_specimens}}) {
        if(exists $tumour->{alignment}->{qc_metrics}) {
          print "\tTUMOUR aliquot_id: ",$tumour->{aliquot_id},"\n";
          print "\t",Dumper($tumour->{alignment}->{qc_metrics}),"\n";
        }
      }
    }
    if(exists $donor->{normal_specimen}->{alignment}->{qc_metrics}) {
      print "\tNORMAL aliquot_id: ",$donor->{normal_specimen}->{aliquot_id},"\n";
      print "\t",Dumper($donor->{normal_specimen}->{alignment}->{qc_metrics}),"\n";
    }
#    print $jsonl;
  }
}

sub header_idclash {
  my ($z, @to_check) = @_;
  my @uniq_donor_ids;

  for(@to_check) {
    push @uniq_donor_ids, load_donors($_);
  }

  my $total_donor_problems = 0;
  my $total_tumour_problems = 0;

  while(my $jsonl = <$z>) {
    my $donor = decode_json($jsonl);
    next if(@to_check > 0 && !first {$_ eq $donor->{donor_unique_id}} @uniq_donor_ids);
    if( first {$_ eq 'sanger'} @{$donor->{flags}->{variant_calling_performed}}) {
      my %norm_rgids;
      for my $norm_qc(@{$donor->{normal_specimen}->{alignment}->{qc_metrics}}) {
        $norm_rgids{ $norm_qc->{read_group_id} } = 1;
      }

      my @result;
      for my $tumour_align(@{$donor->{aligned_tumor_specimens}}) {
        my $tum_aliq = $tumour_align->{aliquot_id};
        my $tum_clash_count = 0;
        for my $tum_qc(@{$tumour_align->{alignment}->{qc_metrics}}) {
          $tum_clash_count++ if(exists $norm_rgids{ $tum_qc->{read_group_id} });
        }
        if($tum_clash_count) {
          push @result, sprintf "\taliquot_id %s has %d readgroup IDs that clash with the matched normal.", $tum_aliq, $tum_clash_count;
          $total_tumour_problems++;
        }
      }
      if(scalar @result > 0) {
        unshift @result, sprintf "Donor: %s", $donor->{donor_unique_id};
        print join("\n", @result),"\n";
        $total_donor_problems++;
      }
    }
  }
  print sprintf "Total donors affected: %d\n", $total_donor_problems;
  print sprintf "Total tumours affected: %d\n", $total_tumour_problems;
}

sub check_analysis {
  my ($z, @to_check) = @_;
  my @uniq_donor_ids;
  for(@to_check) {
    push @uniq_donor_ids, load_donors($_);
  }

  my (@pending, @done);
  while(my $jsonl = <$z>) {
    my $donor = decode_json($jsonl);
    next unless( first {$_ eq $donor->{donor_unique_id}} @uniq_donor_ids);
    if( first {$_ eq 'sanger'} @{$donor->{flags}->{variant_calling_performed}}) {
      push @done, $donor->{donor_unique_id};
    }
    else {
      my $wl_format = $donor->{donor_unique_id};
      $wl_format =~ s/[:]{2}/ /;
      push @pending, $wl_format;
    }

  }

  print join("\n",'Done:',sort @done),"\n";
  print "\n\n";
  print join("\n",'Pending:',sort @pending),"\n";

  return;
}

sub load_donors {
  my $file = shift;
  my @donors;
  open my $IN, '<', $file;
  while(my $line = <$IN>) {
    chomp $line;
    $line =~ s/\s+/::/g;
    push @donors, $line;
  }
  return @donors;
}

sub manifest {
  my $tmpdir = shift;
  my $url = $DEFAULT_URL;
  my $ff = File::Fetch->new(uri => $url, tempdir_root => File::Spec->tmpdir);
  my $listing;
  my $where = $ff->fetch( to => \$listing );
  my ($file) = $listing =~ m/(donor_p_[[:digit:]]+[.]jsonl[.]gz)/xms;
  my $to_get = "$url/$file";
  $ff = File::Fetch->new(uri => $to_get);
  $where = $ff->fetch( to => $tmpdir);
  my $dest = $tmpdir.'/donor_p.jsonl.gz';
  move($where, $dest);
  return $dest;
}




sub donors_by_size {
  my $z = shift;
  my $GB_FAC = 1 / 1024 / 1024 / 1024;

  my $min_size = 1_000_000_000_000_000; # ~1PTB
  my $small_donor;
  print "Donor\tNormalSeqX\tNormalGB\tTumourSeqX\tTumourGB\n";
  while(my $jsonl = <$z>) {
    my $donor = decode_json($jsonl);
    next if($donor->{flags}->{is_donor_blacklisted});
    next if($donor->{flags}->{is_test});
    next if($donor->{flags}->{is_manual_qc_failed});
    next if($donor->{flags}->{is_normal_specimen_aligned} == 0 || $donor->{flags}->{are_all_tumor_specimens_aligned} == 0);
    if(exists $donor->{normal_specimen}->{bam_file_size} && exists $donor->{aligned_tumor_specimens} && @{$donor->{aligned_tumor_specimens}} == 1) {

      my $norm_uniq_seqX = uniq_mapping_cov($donor->{normal_specimen}->{alignment}->{qc_metrics}, $donor->{normal_specimen}->{alignment}->{markduplicates_metrics} );
      my $tum_uniq_seqX = uniq_mapping_cov($donor->{aligned_tumor_specimens}->[0]->{alignment}->{qc_metrics}, $donor->{aligned_tumor_specimens}->[0]->{alignment}->{markduplicates_metrics} );

      print sprintf "%s\t%s\t%.1f\t%s\t%.1f\n", $donor->{donor_unique_id},
                                                $norm_uniq_seqX,
                                                $donor->{normal_specimen}->{bam_file_size} * $GB_FAC,
                                                $tum_uniq_seqX,
                                                $donor->{aligned_tumor_specimens}->[0]->{bam_file_size} * $GB_FAC;


      my $combined_size = $donor->{normal_specimen}->{bam_file_size} + $donor->{aligned_tumor_specimens}->[0]->{bam_file_size};
      if( $combined_size < $min_size ) {
        $min_size = $combined_size;
        $small_donor = $donor;
      }
    }
  }

  #print sprintf "%s\tTumour\t%.1f\tNormal\t%.1f\n", $small_donor->{donor_unique_id},
  #                                                  $small_donor->{normal_specimen}->{bam_file_size} * $GB_FAC,
  #                                                  $small_donor->{aligned_tumor_specimens}->[0]->{bam_file_size} * $GB_FAC;

  my $gb = $min_size * $GB_FAC;
  print sprintf "Smallest single tumour donor: %s (%.2f GB)\n", $small_donor->{donor_unique_id}, $gb;
}


sub uniq_mapping_cov {
  my ($qc_by_lane, $dup_by_lib) = @_;
#warn Dumper($qc_by_lane);
#warn Dumper($dup_by_lib);
#exit;
  my %lib_mapped_bp;
  for(@{$qc_by_lane}) {
    $lib_mapped_bp{$_->{metrics}->{library}} += $_->{metrics}->{'#_mapped_bases'};
  }
  my $uniq_seq_bp = 0;
  for(@{$dup_by_lib}) {
    $uniq_seq_bp += $lib_mapped_bp{$_->{library}} * (1- $_->{metrics}->{percent_duplication});
  }
  return sprintf "%.2f", $uniq_seq_bp / 3_000_000_000;
}
