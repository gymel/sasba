# -*- perl -*-

# t/020_stats.t - check dumps and maintenance 

use Test::More tests => 4;

BEGIN { 
  use_ok( 'SeeAlso::Source::BeaconAggregator::Maintenance' );
}

# open database

my $dsn = "testdb";

my $use = SeeAlso::Source::BeaconAggregator::Maintenance->new(dsn => $dsn);
ok (defined $use, "accessed db with dsn");
isa_ok ($use, 'SeeAlso::Source::BeaconAggregator');

# findExample
subtest 'findExample' => sub {
	plan tests => 9;
	my $resultref = $use->findExample(4);
	is($resultref, undef, 'empty result on impossible request');

        my $sth = "";
	$resultref = $use->findExample(2, 0, $sth);
	isa_ok($resultref, 'HASH', 'fulfilled request');
	ok($sth, 'statement handle to pass back');
        my $expected = { id => '103117741', response => '3/0' };
	is_deeply($resultref, $expected, 'expected identifier and counts');

	$resultref = $use->findExample(2, 1, $sth);
	isa_ok($resultref, 'HASH', 'fulfilled sequential request');
	ok($sth, 'statement handle to pass back');
        my $expected2 = { id => '118784226', response => '2/0' };
	is_deeply($resultref, $expected2, 'expected identifier and counts');

	$resultref = $use->findExample(2, 2, $sth);
	is($resultref, undef, 'unsatisfied sequential request');
	ok($sth, 'statement handle to pass back');
};


