# -*- perl -*-

# t/020_stats.t - check dumps and maintenance 

use Test::More tests => 9;

BEGIN { 
  use_ok( 'SeeAlso::Source::BeaconAggregator::Maintenance' );
}

# open database

my $dsn = "testdb";

my $use = SeeAlso::Source::BeaconAggregator::Maintenance->new(dsn => $dsn);
ok (defined $use, "accessed db with dsn");
isa_ok ($use, 'SeeAlso::Source::BeaconAggregator');

# idStat
subtest 'idStat' => sub {
	plan tests => 6;
	my $itot = $use->idStat();
	ok($itot, 'nonzero idStat');
	is($itot, 10, 'idStat returned unexpected count');
# idStat distinct
	$itot = $use->idStat(0, (distinct => 1));
	ok($itot, 'nonzero distinct idStat');
	is($itot, 7, ' distinct idStat returned unexpected count');
# idStat for one alias
	$itot = $use->idStat('foo');
	ok($itot, 'nonzero idStat for foo');
	is($itot, 3, 'idStat for foo returned unexpected count');
};

# cached idStat
subtest 'cachedStat' => sub {
	plan tests => 2;
        my %adm = %{$use->admhash};
	my $itot = $use->idStat();
        is($adm{'gcounti'}, $itot, ' cached identifier count differs from live');
	my $utot = $use->idStat(0, (distinct => 1));
        is($adm{'gcountu'}, $utot, ' cached unique identifier count differs from live');
};

# idCounts
subtest 'idCounts' => sub {
	plan tests => 22;
	my %cexpected = (
	  '118784226' => [2, 0],
	  '132464462' => [1, 1],
	  '118624458' => [1, 2],
	  '103117741' => [3, 0],
	  '118559796' => [1, 0],
	  '10000022-8' => [1, 0],
	  '100001718' => [1, 0],
	);
	while ( my (@clist) = $use->idCounts() ) {
            my $id;
            ok($id = shift @clist, 'got list with id');
	    my $testref;
	    ok($testref = $cexpected{$id}, "idCounts returned unexpected identifier $id");
	    is_deeply(\@clist, $testref, "expected result for $id");
	    delete $cexpected{$id};
	  }
	is(scalar keys %cexpected, 0, "undelivered identifiers");
};

subtest 'idCounts distinct' => sub {
	plan tests => 22;
	my %cexpected = (
	  '118784226' => [2, 0],
	  '132464462' => [1, 1],
	  '118624458' => [1, 2],
	  '103117741' => [1, 0],
	  '118559796' => [1, 0],
	  '10000022-8' => [1, 0],
	  '100001718' => [1, 0],
	);
	while ( my (@clist) = $use->idCounts(0, (distinct => 1)) ) {
            my $id;
            ok($id = shift @clist, 'got list with id');
	    my $testref;
	    ok($testref = $cexpected{$id}, "idCounts returned unexpected identifier $id");
	    is_deeply(\@clist, $testref, "expected result for $id");
	    delete $cexpected{$id};
	  }
	is(scalar keys %cexpected, 0, "undelivered identifiers");
};

# idList
subtest 'idList' => sub {
	plan tests => 45;
	my %iexpected = (
	  '118784226' => {"1:" => ["", "", "", ""], 
        	          "6:" => ["", "de.wikisource.org", "http://toolserver.org/~apper/pd/person/pnd-redirect/ws/118784226", ""]
                	 },
	  '132464462' => {"1:" => [1, "", "", ""]},
	  '118624458' => {"1:" => [2, "", "", ""]},
	  '103117741' => {"6:45433" => ["", "Châtelain, Jean-Jacques", "", "45433"],
	                  "6:Tâtâ" => ["", "Test encoding only", "", "Tâtâ"],
        	          "6:45432" => ["", "Châtelain, Jacques-Jean", "", "45432"],
	                 },
	  '118559796' => {"6:" =>, ["", "", "", ""]},
	  '10000022-8' => {"5:086327216" => ["", "", "", "086327216"]},
	  '100001718' => {"5:117503258" => ["", "Q533022", "", "117503258"]},
	);
	while ( my (@ilist) = $use->idList() ) {
	    ok(@ilist > 1, 'idList gave a tuple');
	    my $id = shift @ilist;
	    my $testref;
	    ok($testref = $iexpected{$id}, "idList returned unexpected identifier $id");
	    $testref ||= {};
	    while ( my $rowref = shift @ilist ) {;
	        my $seqno;
	        ok($seqno = shift @$rowref, 'defined seqno');
	        my $altid = $rowref->[3] || "";
	        my $seqref;
	        ok($seqref = $testref->{"$seqno:$altid"}, "idList returned unexpected seqno:altid $seqno:$altid");
                is_deeply($rowref, $seqref, "expected result for $id/$seqno:$altid");
	        delete $testref->{"$seqno:$altid"};
	      }
	    delete $iexpected{$id} unless keys %{$iexpected{$id}};
	  }
	is(scalar keys %iexpected, 0, "all eaten up");
};

# idList with pattern
subtest 'idList with pattern' => sub {
	plan tests => 11;
	my %iexpected = (
	  '132464462' => {"1:" => [1, "", "", ""]},
	  '118624458' => {"1:" => [2, "", "", ""]},
	);
	while ( my (@ilist) = $use->idList("%44%") ) {
	    ok(@ilist > 1, 'idList with pattern gave a tuple');
	    my $id = shift @ilist;
	    my $testref;
	    ok($testref = $iexpected{$id}, "idList with pattern returned unexpected identifier $id");
	    while ( my $rowref = shift @ilist ) {;
        	my $seqno;
	        ok($seqno = shift @$rowref, 'defined seqno');
        	my $altid = $rowref->[3];
	        my $seqref;
	        ok($seqref = $testref->{"$seqno:$altid"}, "idList with pattern returned unexpected seqno:altid $seqno:$altid");
	        $seqref ||= [];
                is_deeply($rowref, $seqref, "expected result for $id/$seqno:$altid");
	        delete $testref->{"$seqno:$altid"};
	      }
	    delete $iexpected{$id} unless keys %{$iexpected{$id}};
	  }
	is(scalar keys %iexpected, 0, "all eaten up");
};

