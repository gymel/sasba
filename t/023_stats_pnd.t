# -*- perl -*-

# t/020_stats_id.t - check dumps and maintenance 

use Test::More tests => 125;

BEGIN { 
  use_ok( 'SeeAlso::Source::BeaconAggregator::Maintenance' );
  use_ok( 'SeeAlso::Identifier::GND' );
}

# open database

SKIP: {
  eval { require SeeAlso::Identifier::PND };
  skip "SeeAlso::Identifier::PND is not installed", 123 if $@;

my $dsn = "testdb";
my $idclass = SeeAlso::Identifier::PND->new();
ok (defined $idclass, "created identifier object");
isa_ok ($idclass, 'SeeAlso::Identifier::PND');
isa_ok ($idclass, 'SeeAlso::Identifier::GND');
isa_ok ($idclass, 'SeeAlso::Identifier');

my $use = SeeAlso::Source::BeaconAggregator::Maintenance->new(dsn => $dsn, identifierClass => $idclass);
ok (defined $use, "accessed db with dsn");
isa_ok ($use, 'SeeAlso::Source::BeaconAggregator');

# idStat
my $itot = $use->idStat();
ok($itot, 'nonzero idStat');
is($itot, 7, 'idStat returned unexpected count');
# idStat distinct
$itot = $use->idStat(0, (distinct => 1));
ok($itot, 'nonzero distinct idStat');
is($itot, 5, ' distinct idStat returned unexpected count');
# idStat for one alias
$itot = $use->idStat('foo');
ok($itot, 'nonzero idStat for foo');
is($itot, 3, 'idStat for foo returned unexpected count');


# idCounts
my %cexpected = (
  '118784226' => [2, 0],
  '132464462' => [1, 1],
  '118624458' => [1, 2],
  '103117741' => [2, 0],
  '118559796' => [1, 0],
);
while ( my (@clist) = $use->idCounts() ) {
    my $testref;
    ok($testref = $cexpected{$clist[0]}, "idCounts returned unexpected identifier $clist[0]");
    $testref ||= [];
    is($clist[1], $testref->[0], "[idCounts $clist[0]]: (count)");
    is($clist[2], $testref->[1], "[idCounts $clist[0]]: (sum)");
    delete $cexpected{$clist[0]};
  }
my @cexcess = keys %cexpected;
is("@cexcess", "", "undelivered identifiers for idCounts");

%cexpected = (
  '118784226' => [2, 0],
  '132464462' => [1, 1],
  '118624458' => [1, 2],
  '103117741' => [1, 0],
  '118559796' => [1, 0],
);
while ( my (@clist) = $use->idCounts(0, (distinct => 1)) ) {
    my $testref;
    ok($testref = $cexpected{$clist[0]}, "distinct idCounts returned unexpected identifier $clist[0]");
    $testref ||= [];
    is($clist[1], $testref->[0], "[distinct idCounts $clist[0]]: (count)");
    is($clist[2], $testref->[1], "[distinct idCounts $clist[0]]: (sum)");
    delete $cexpected{$clist[0]};
  }
@cexcess = keys %cexpected;
is("@cexcess", "", "undelivered identifiers for distinct idCounts");


# idList
my %iexpected = (
  '118784226' => {"1:" => ["", "", "", ""], 
                  "3:" => ["", "de.wikisource.org", "http://toolserver.org/~apper/pd/person/pnd-redirect/ws/118784226", ""]
                 },
  '132464462' => {"1:" => [1, "", "", ""]},
  '118624458' => {"1:" => [2, "", "", ""]},
  '103117741' => {"3:45433" => ["", "Châtelain, Jean-Jacques", "", "45433"],
                  "3:45432" => ["", "Châtelain, Jacques-Jean", "", "45432"]
                 },
  '118559796' => {"3:" =>, ["", "", "", ""]},
);
while ( my (@ilist) = $use->idList() ) {
    ok(@ilist > 1, 'idList gave a tuple');
    my $id = shift @ilist;
    my $testref;
    ok($testref = $iexpected{$id}, "idList returned unexpected identifier $id");
    $testref ||= {};
    while ( my $rowref = shift @ilist ) {;
        isa_ok($rowref, ARRAY, 'we got a list');
        is(scalar @$rowref, 5, 'list has five elements');
        my $seqno;
        ok($seqno = $rowref->[0], 'defined seqno');
        my $altid = $rowref->[4];
        my $seqref;
        ok($seqref = $testref->{"$seqno:$altid"}, "idList returned unexpected seqno:altid $seqno:$altid");
        $seqref ||= [];
        is($rowref->[1], $seqref->[0], "[idList $id/$seqno:$altid]:  (hits)");
        is($rowref->[2], $seqref->[1], "[idList $id/$seqno:$altid]:  (info)");
        is($rowref->[3], $seqref->[2], "[idList $id/$seqno:$altid]:  (link)");
#       is($rowref->[4], $seqref->[3], "[idList $id/$seqno:$altid]:  (altid)");
        delete $testref->{"$seqno:$altid"};
      }
    delete $iexpected{$id} unless keys %{$iexpected{$id}};
  }
my @iexcess = keys %iexpected;
is("@iexcess", "", "undelivered identifiers from idList");

# idList with pattern
%iexpected = (
  '132464462' => {"1:" => [1, "", ""]},
  '118624458' => {"1:" => [2, "", ""]},
);
while ( my (@ilist) = $use->idList("%44%") ) {
    ok(@ilist > 1, 'idList with pattern gave a tuple');
    my $id = shift @ilist;
    my $testref;
    ok($testref = $iexpected{$id}, "idList with pattern returned unexpected identifier $id");
    $testref ||= {};
    while ( my $rowref = shift @ilist ) {;
        isa_ok($rowref, ARRAY, 'we got a list');
        is(scalar @$rowref, 5, 'list has five elements');
        my $seqno;
        ok($seqno = $rowref->[0], 'defined seqno');
        my $altid = $rowref->[4];
        my $seqref;
        ok($seqref = $testref->{"$seqno:$altid"}, "idList with pattern returned unexpected seqno:altid $seqno:$altid");
        $seqref ||= [];
        is($rowref->[1], $seqref->[0], "[idList with pattern $id/$seqno:$altid]:  (hits)");
        is($rowref->[2], $seqref->[1], "[idList with pattern $id/$seqno:$altid]:  (info)");
        is($rowref->[3], $seqref->[2], "[idList with pattern $id/$seqno:$altid]:  (link)");
#       is($rowref->[4], $seqref->[3], "[idList with pattern $id/$seqno:$altid]:  (altid)");
        delete $testref->{"$seqno:$altid"};
      }
    delete $iexpected{$id} unless keys %{$iexpected{$id}};
  }
@iexcess = keys %iexpected;
is("@iexcess", "", "undelivered identifiers from idList with pattern");

} # END OF SKIP

