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
	plan tests => 6;
	is($use->setOSD('Tags', 'old hits'), 1, 'setOSD');
	is($use->setOSD('Tags', 'hits', 'more hits'), 2, 'setOSD multiple');
	is($use->setOSD('Attribution'), 0, 'emtpy field for setOSD');
	is($use->addOSD('Attribution', ""), 1, 'emtpy value for setOSD');
	is($use->setOSD('foobar', 'xxx'), undef, 'illegal field setOSD');
	is($use->setOSD(), undef, 'empty setOSD');
};

# getOSD
subtest 'addOSD' => sub {
	plan tests => 7;
	is($use->addOSD('Tags', 'some encountered'), 1, 'addOSD');
	is($use->addOSD('Tags', "added more hits", "added even more hits"), 2, 'multiple fields for addOSD');
	is($use->addOSD('Tags', "added more hits", "added last hits"), 2, 'multiple fields with overlapping values for addOSD');
	is($use->addOSD('Attribution'), 0, 'emtpy field for addOSD');
	is($use->addOSD('Attribution', ""), 1, 'emtpy value for addOSD');
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
		'Tags' => ["hits", "more hits",
                           "some encountered",
                           "added more hits", "added even more hits",
                           "added more hits", "added last hits",
                          ],
	};

	my $osd = $use->OSDValues();
	is(ref($osd), 'HASH', 'return type of OSDValues');
	is_deeply($osd, $expected, 'OSDValues as expected');
}

