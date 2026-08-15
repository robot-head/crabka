#!/usr/bin/env perl
use strict;
use warnings FATAL => 'all';

my $catalog_dir = shift // die "usage: $0 POSTGRES_CATALOG_DIR\n";
unshift @INC, "$catalog_dir/../../backend/catalog";
require Catalog;

my $header = Catalog::ParseHeader("$catalog_dir/pg_proc.h");
my $rows = Catalog::ParseData("$catalog_dir/pg_proc.dat", $header->{columns}, 0);
my %by_signature;
my %descriptions;
for my $row (@$rows)
{
    $by_signature{"$row->{proname}|$row->{proargtypes}"} = $row->{oid};
    $descriptions{$row->{oid}} = $row->{descr} if defined $row->{descr};
}

my $operator_header = Catalog::ParseHeader("$catalog_dir/pg_operator.h");
my $operators = Catalog::ParseData(
    "$catalog_dir/pg_operator.dat", $operator_header->{columns}, 0);
for my $operator (@$operators)
{
    next if defined $operator->{descr} && $operator->{descr} =~ /^deprecated/;
    my ($name, $arguments) = $operator->{oprcode} =~ /^([^()]+)\((.*)\)$/;
    if (!defined $name)
    {
        $name = $operator->{oprcode};
        $arguments = join ' ', grep { $_ ne '0' } ($operator->{oprleft}, $operator->{oprright});
    }
    else
    {
        $arguments =~ tr/,/ /;
    }
    my $oid = $by_signature{"$name|$arguments"};
    $descriptions{$oid} //= "implementation of $operator->{oprname} operator"
        if defined $oid;
}

for my $oid (sort { $a <=> $b } keys %descriptions)
{
    my $description = $descriptions{$oid};
    die "description contains a tab or newline\n" if $description =~ /[\t\n]/;
    print "$oid\t$description\n";
}
