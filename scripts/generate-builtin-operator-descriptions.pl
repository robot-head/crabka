#!/usr/bin/env perl
use strict;
use warnings FATAL => 'all';

my $catalog_dir = shift // die "usage: $0 POSTGRES_CATALOG_DIR\n";
unshift @INC, "$catalog_dir/../../backend/catalog";
require Catalog;

my $header = Catalog::ParseHeader("$catalog_dir/pg_operator.h");
my $rows = Catalog::ParseData("$catalog_dir/pg_operator.dat", $header->{columns}, 0);
for my $row (@$rows)
{
    die "operator description is missing\n" unless defined $row->{descr};
    die "description contains a tab or newline\n" if $row->{descr} =~ /[\t\n]/;
    print "$row->{oid}\t$row->{descr}\n";
}
