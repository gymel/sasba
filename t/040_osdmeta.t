# -*- perl -*-

# t/045_osd_meta.t - check manipulation of OSD values

use Test::More tests => 7;

BEGIN { 
  use_ok( 'SeeAlso::Source::BeaconAggregator::Maintenance' );
}

# create new database

my $dsn = "testdb";

my $use = SeeAlso::Source::BeaconAggregator::Maintenance->new(dsn => $dsn);
ok (defined $use, "accessed db with dsn");
isa_ok ($use, 'SeeAlso::Source::BeaconAggregator');

# setOSD
subtest 'setOSD' => sub {
	plan tests => 4;
	is($use->setOSD('Tags', 'hits'), 1, 'setOSD');
	is($use->setOSD('Attribution'), 0, 'emtpy field for setOSD');
	is($use->setOSD('foobar', 'xxx'), undef, 'illegal field setOSD');
	is($use->setOSD(), undef, 'empty setOSD');
};

# getOSD
subtest 'getOSD' => sub {
	plan tests => 6;
	is($use->addOSD('Tags', 'some encountered'), 1, 'addOSD');
	is($use->addOSD('Attribution', "hit"), 1, 'emtpy field for addOSD');
	is($use->addOSD('Attribution', "more hits"), 1, 'emtpy field for addOSD');
	is($use->addOSD('Attribution'), 1, 'empty addOSD');
	is($use->addOSD('foobar', 'xxx'), undef, 'illegal field addOSD');
	is($use->addOSD(), undef, 'empty addOSD');
};

# clearOSD
subtest 'clearOSD' => sub {
	plan tests => 3;
	is($use->clearOSD('Attribution'), 1, 'clearOSD');
	is($use->clearOSD('foobar'), undef, 'illegal field clearOSD');
	is($use->clearOSD(), undef, 'empty clearOSD');
};

# OSDValues
subtest 'OSDValues' => sub {
	plan tests => 2;

	my $expected = {
		'Tags' => ["hits", "some encountered"],
	};

	my $osd = $use->OSDValues();
	is(ref($osd), 'HASH', 'return type of OSDValues');
	is_deeply($osd, $expected, 'OSDValues as expected');
}




