#!/usr/bin/env perl
use strict;
use warnings FATAL => 'all';

use File::Compare qw(compare);
use File::Spec;
use File::Temp qw(tempdir);
use FindBin;

my ($check, $live) = (0, 0);
while (@ARGV && $ARGV[0] =~ /^--(?:check|live)$/)
{
    my $option = shift @ARGV;
    $check = 1 if $option eq '--check';
    $live = 1 if $option eq '--live';
}
my $default_fixture_dir =
  File::Spec->catdir($FindBin::Bin, '..', 'crates', 'pgexec', 'src');

if ($live)
{
    my $fixture_dir = shift @ARGV // $default_fixture_dir;
    die "usage: $0 [--check] --live [FIXTURE_DIR]\n" if @ARGV;

    # pg_proc.dat is not the final catalog. initdb runs system_functions.sql,
    # which adds routines and installs argument names/defaults on existing
    # entries. Read an initialized PostgreSQL 18 server through the standard
    # libpq environment (PGHOST, PGPORT, PGUSER) so this captures that catalog.
    my $query = <<'SQL';
SELECT oid, proname, prolang, procost::int, prorows::int, provariadic,
       CASE WHEN prosupport = 0 THEN '0' ELSE prosupport::regproc::text END, ascii(prokind),
       (CASE WHEN prosecdef THEN '1' ELSE '0' END) ||
       (CASE WHEN proleakproof THEN '1' ELSE '0' END) ||
       (CASE WHEN proisstrict THEN '1' ELSE '0' END) ||
       (CASE WHEN proretset THEN '1' ELSE '0' END),
       ascii(provolatile), ascii(proparallel), pronargs, pronargdefaults,
       prorettype, proargtypes::text, prosrc, coalesce(prosqlbody::text, '-'),
       coalesce(proargmodes::text, '-'), coalesce(proallargtypes::text, '-'),
       coalesce(proargnames::text, '-'), coalesce(pg_get_expr(proargdefaults, 0), '-')
FROM pg_proc
ORDER BY oid
SQL
    open my $input, '-|', 'psql', '-XAt', '-F', "\t", '-v',
      'ON_ERROR_STOP=1', '-d', 'postgres', '-c', $query
      or die "cannot read initialized PostgreSQL catalog: $!\n";
    my @rows;
    while (my $line = <$input>)
    {
        chomp $line;
        my @fields = split /\t/, $line, -1;
        die "live pg_proc row has " . scalar(@fields) . " fields, expected 21\n"
          unless @fields == 21;
        die "live pg_proc row has unsafe field\n"
          if grep { /[\t\n]/ } @fields;
        push @rows, join("\t", @fields) . "\n";
    }
    close $input or die "psql failed while reading initialized PostgreSQL catalog\n";
    die "expected 3413 initialized pg_proc rows, got " . scalar(@rows) . "\n"
      unless @rows == 3413;

    my $temporary = tempdir('builtin-procs.XXXXXX', DIR => $fixture_dir, CLEANUP => 1);
    my @generated;
    my $chunk = int((@rows + 3) / 4);
    for my $index (0 .. 3)
    {
        my $start = $index * $chunk;
        my $count = $index == 3 ? @rows - $start : $chunk;
        my $tsv = File::Spec->catfile($temporary, "builtin_procs_$index.tsv");
        my $compressed = "$tsv.zst";
        open my $output, '>', $tsv or die "cannot write $tsv: $!\n";
        print {$output} @rows[$start .. $start + $count - 1];
        close $output or die "cannot close $tsv: $!\n";
        system 'zstd', '--quiet', '--force', '-19', $tsv, '-o', $compressed;
        die "zstd failed for chunk $index\n" if $? != 0;
        chmod 0644, $compressed or die "cannot chmod $compressed: $!\n";
        push @generated, $compressed;
    }
    for my $index (0 .. 3)
    {
        my $path = File::Spec->catfile($fixture_dir, "builtin_procs_$index.tsv.zst");
        if ($check)
        {
            die "$path is not reproducible\n" unless compare($path, $generated[$index]) == 0;
        }
        else
        {
            rename $generated[$index], $path or die "cannot replace $path: $!\n";
        }
    }
    print $check
      ? "verified 3413 initialized pg_proc rows in 854/854/854/851-row chunks\n"
      : "generated 3413 initialized pg_proc rows in 854/854/854/851-row chunks\n";
    exit 0;
}

my $catalog_dir = shift // die
  "usage: $0 [--check] POSTGRES_CATALOG_DIR [FIXTURE_DIR]\n";
my $fixture_dir = shift // $default_fixture_dir;
die "usage: $0 [--check] POSTGRES_CATALOG_DIR [FIXTURE_DIR]\n" if @ARGV;

unshift @INC, "$catalog_dir/../../backend/catalog";
require Catalog;

my $type_header = Catalog::ParseHeader("$catalog_dir/pg_type.h");
my $type_rows = Catalog::ParseData(
    "$catalog_dir/pg_type.dat", $type_header->{columns}, 0);
my (%type_oids, %type_names);
for my $row (@$type_rows)
{
    my ($oid, $name) = @{$row}{qw(oid typname)};
    die "pg_type.dat row has invalid oid\n"
      unless defined $oid && $oid =~ /^\d+$/;
    die "pg_type.dat oid $oid has no typname\n" unless defined $name;
    die "duplicate pg_type.dat oid $oid\n" if $type_oids{$oid}++;
    die "duplicate pg_type.dat typname $name\n" if exists $type_names{$name};
    $type_names{$name} = $oid;
}

my $header = Catalog::ParseHeader("$catalog_dir/pg_proc.h");
my $catalog_rows = Catalog::ParseData(
    "$catalog_dir/pg_proc.dat", $header->{columns}, 0);
die "expected 3397 pg_proc.dat rows, got " . scalar(@$catalog_rows) . "\n"
  unless @$catalog_rows == 3397;

my %catalog_by_oid;
for my $row (@$catalog_rows)
{
    my $oid = $row->{oid};
    die "pg_proc.dat row has no oid\n" unless defined $oid;
    die "duplicate pg_proc.dat oid $oid\n" if exists $catalog_by_oid{$oid};
    die "pg_proc.dat oid $oid has no prosrc\n" unless defined $row->{prosrc};
    die "pg_proc.dat oid $oid has unsafe prosrc\n" if $row->{prosrc} =~ /[\t\n]/;
    $catalog_by_oid{$oid} = $row;
}

my @fixture_paths = map {
    File::Spec->catfile($fixture_dir, "builtin_procs_$_.tsv.zst")
} 0 .. 3;
my @rows;
my %fixture_oids;
for my $index (0 .. $#fixture_paths)
{
    my $path = $fixture_paths[$index];
    open my $input, '-|', 'zstd', '--quiet', '--decompress', '--stdout', '--', $path
      or die "cannot decompress $path: $!\n";
    my $chunk_rows = 0;
    while (my $line = <$input>)
    {
        chomp $line;
        my @fields = split /\t/, $line, -1;
        die "$path has " . scalar(@fields) . " fields, expected 19\n"
          unless @fields == 19;
        my $oid = $fields[0];
        die "$path has invalid oid $oid\n" unless $oid =~ /^\d+$/;
        die "duplicate fixture oid $oid\n" if $fixture_oids{$oid}++;
        my $catalog = $catalog_by_oid{$oid}
          // die "fixture oid $oid is absent from pg_proc.dat\n";
        die "fixture oid $oid name mismatch: $fields[1] != $catalog->{proname}\n"
          unless $fields[1] eq $catalog->{proname};
        die "fixture oid $oid prosrc mismatch: $fields[15] != $catalog->{prosrc}\n"
          unless $fields[15] eq $catalog->{prosrc};
        my $arg_modes = $catalog->{proargmodes};
        $arg_modes = '-' if !defined $arg_modes || $arg_modes eq '_null_';
        die "pg_proc.dat oid $oid has unsafe proargmodes\n"
          if $arg_modes =~ /[\t\n]/;
        die "fixture oid $oid proargmodes mismatch: $fields[16] != $arg_modes\n"
          unless $fields[16] eq $arg_modes;
        my $all_arg_types = $catalog->{proallargtypes};
        if (!defined $all_arg_types || $all_arg_types eq '_null_')
        {
            $all_arg_types = '-';
        }
        else
        {
            die "pg_proc.dat oid $oid has invalid proallargtypes\n"
              unless $all_arg_types =~ /^\{[a-zA-Z0-9_]+(?:,[a-zA-Z0-9_]+)*\}$/;
            my @names = split /,/, substr($all_arg_types, 1, -1);
            my @oids = map {
                $type_names{$_}
                  // die "pg_proc.dat oid $oid has unknown proallargtype $_\n"
            } @names;
            $all_arg_types = '{' . join(',', @oids) . '}';
        }
        my $arg_names = $catalog->{proargnames};
        $arg_names = '-'
          if !defined $arg_names || $arg_names eq '_null_';
        die "pg_proc.dat oid $oid has invalid proargnames\n"
          unless $arg_names eq '-' || $arg_names =~ /^\{.*\}$/;
        die "pg_proc.dat oid $oid has unsafe proargnames\n"
          if $arg_names =~ /[\t\n]/;
        die "fixture oid $oid proargnames mismatch: $fields[18] != $arg_names\n"
          unless $fields[18] eq $arg_names;
        $fields[17] = $all_arg_types;
        push @rows, join("\t", @fields) . "\n";
        $chunk_rows++;
    }
    close $input or die "zstd failed while reading $path\n";
    my $expected = $index < 3 ? 850 : 847;
    die "$path has $chunk_rows rows, expected $expected\n"
      unless $chunk_rows == $expected;
}

die "expected 3397 fixture rows, got " . scalar(@rows) . "\n"
  unless @rows == 3397;
for my $oid (keys %catalog_by_oid)
{
    die "pg_proc.dat oid $oid is absent from fixtures\n"
      unless exists $fixture_oids{$oid};
}

my $temporary = tempdir('builtin-procs.XXXXXX', DIR => $fixture_dir, CLEANUP => 1);
my @generated;
for my $index (0 .. 3)
{
    my $start = $index * 850;
    my $count = $index < 3 ? 850 : 847;
    my $tsv = File::Spec->catfile($temporary, "builtin_procs_$index.tsv");
    my $compressed = "$tsv.zst";
    open my $output, '>', $tsv or die "cannot write $tsv: $!\n";
    print {$output} @rows[$start .. $start + $count - 1];
    close $output or die "cannot close $tsv: $!\n";
    system 'zstd', '--quiet', '--force', '-19', $tsv, '-o', $compressed;
    die "zstd failed for chunk $index\n" if $? != 0;
    chmod 0644, $compressed or die "cannot chmod $compressed: $!\n";
    push @generated, $compressed;
}

if ($check)
{
    for my $index (0 .. 3)
    {
        die "$fixture_paths[$index] is not reproducible\n"
          unless compare($fixture_paths[$index], $generated[$index]) == 0;
    }
    print "verified 3397 pg_proc rows in 850/850/850/847-row chunks\n";
    exit 0;
}

for my $index (0 .. 3)
{
    rename $generated[$index], $fixture_paths[$index]
      or die "cannot replace $fixture_paths[$index]: $!\n";
}
print "generated 3397 pg_proc rows in 850/850/850/847-row chunks\n";
