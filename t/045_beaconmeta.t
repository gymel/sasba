# -*- perl -*-

# t/045_beaconmeta.t - check manipulation of beacon headers

use Test::More tests => 16;

BEGIN { 
  use_ok( 'SeeAlso::Source::BeaconAggregator::Maintenance' );
}

# create new database

my $dsn = "testdb";

my $use = SeeAlso::Source::BeaconAggregator::Maintenance->new(dsn => $dsn);
ok (defined $use, "accessed db with dsn");
isa_ok ($use, 'SeeAlso::Source::BeaconAggregator');

# setBeaconMeta
is($use->setBeaconMeta('MESSAGE', '{ID} hits in test repo'), 1, 'setBeaconMeta');
is($use->setBeaconMeta('ONEMESSAGE'), 0, 'emtpy field for setBeaconMeta');
is($use->setBeaconMeta('ROGUE', 'xxx'), undef, 'illegal field setBeaconMeta');
is($use->setBeaconMeta(), undef, 'empty setBeaconMeta');

# control


# getBeaconMeta
is($use->addBeaconMeta('MESSAGE', ' encountered'), 1, 'addBeaconMeta');
is($use->addBeaconMeta('ONEMESSAGE', "hit"), 1, 'add field for addBeaconMeta');
is($use->addBeaconMeta('ONEMESSAGE', ", hit"), 1, 'add field for addBeaconMeta');
is($use->addBeaconMeta('ONEMESSAGE'), 1, 'empty addBeaconMeta');
is($use->addBeaconMeta('ROGUE', 'xxx'), undef, 'illegal field addBeaconMeta');
is($use->addBeaconMeta(), undef, 'empty addBeaconMeta');

# control

# clearBeaconMeta
is($use->clearBeaconMeta('ONEMESSAGE'), 1, 'clearBeaconMeta');
is($use->clearBeaconMeta('ROGUE'), undef, 'illegal field clearBeaconMeta');
is($use->clearBeaconMeta(), undef, 'empty clearBeaconMeta');

# control



