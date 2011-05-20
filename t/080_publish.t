# -*- perl -*-

# t/080_publish.t - publisher functions: 

use Test::More tests => 4;

BEGIN { 
  use_ok( 'SeeAlso::Source::BeaconAggregator::Publisher' );
}

# create new database

my $dsn = "testdb";

my $use = SeeAlso::Source::BeaconAggregator::Publisher->new(dsn => $dsn);
ok (defined $use, "accessed db with dsn");
isa_ok ($use, 'SeeAlso::Source::BeaconAggregator');
isa_ok ($use, 'SeeAlso::Source::BeaconAggregator::Publisher');


# redirect


# sources




