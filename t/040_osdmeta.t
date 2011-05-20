# -*- perl -*-

# t/045_osd_meta.t - check manipulation of OSD values

use Test::More tests => 16;

BEGIN { 
  use_ok( 'SeeAlso::Source::BeaconAggregator::Maintenance' );
}

# create new database

my $dsn = "testdb";

my $use = SeeAlso::Source::BeaconAggregator::Maintenance->new(dsn => $dsn);
ok (defined $use, "accessed db with dsn");
isa_ok ($use, 'SeeAlso::Source::BeaconAggregator');

# setOSD
is($use->setOSD('Tags', 'hits'), 1, 'setOSD');
is($use->setOSD('Attribution'), 0, 'emtpy field for setOSD');
is($use->setOSD('foobar', 'xxx'), undef, 'illegal field setOSD');
is($use->setOSD(), undef, 'empty setOSD');

# control


# getOSD
is($use->addOSD('Tags', 'some encountered'), 1, 'addOSD');
is($use->addOSD('Attribution', "hit"), 1, 'emtpy field for addOSD');
is($use->addOSD('Attribution', "more hits"), 1, 'emtpy field for addOSD');
is($use->addOSD('Attribution'), 1, 'empty addOSD');
is($use->addOSD('foobar', 'xxx'), undef, 'illegal field addOSD');
is($use->addOSD(), undef, 'empty addOSD');

# control

# clearOSD
is($use->clearOSD('Attribution'), 1, 'clearOSD');
is($use->clearOSD('foobar'), undef, 'illegal field clearOSD');
is($use->clearOSD(), undef, 'empty clearOSD');

# control



