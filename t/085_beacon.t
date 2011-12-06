# -*- perl -*-

# t/085_beacon.t - check beacon export

use Test::More tests => 6;

BEGIN { 
  use_ok( 'SeeAlso::Source::BeaconAggregator::Publisher' );
}

# create new database

my $dsn = "testdb";

my $use = SeeAlso::Source::BeaconAggregator::Publisher->new(dsn => $dsn);
ok (defined $use, "accessed db with dsn");
isa_ok ($use, 'SeeAlso::Source::BeaconAggregator');
isa_ok ($use, 'SeeAlso::Source::BeaconAggregator::Publisher');

my $expect = << "XxX";
#INSTITUTION: Example Corp, http://www.example.com
#FORMAT: BEACON
#VERSION: 0.1
#TARGET: http://beacon.example.com/test/?format=sources&id={ID}
#TIMESTAMP: 2011-05-19T15:49:19Z
#FEED: http://beacon.example.com/test/?format=beacon
#MESSAGE: encountered
#X-REVISION: 3 [2011-05-19T21:21:04Z]
XxX

my $tpattern = '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z';
my %hexpected;

# dumpmeta
subtest "dumpmeta w/o REVISIT" => sub {
    plan tests => 27;

    my($error, $resultref) = $use->dumpmeta( # cgibase unAPIformatname headers_only {preset}
        "http://beacon.example.com/test/",
        undef,         # defaults to "sources"
        1,
        {
            'INSTITUTION' => "Example Corp, http://www.example.com",
        }
      );
    is($error, 0, 'no error');
    isa_ok($resultref, 'ARRAY', 'returned content');

    my %expecthash;
    while ( $expect =~ /^#([A-Z][A-Z0-9-]*):\s*(.*)$/gm ) {
        my($key, $val) = ($1, $2);
        $val =~ s/\s+$//g;
        $expecthash{$key} = quotemeta($val);
      };
    $expecthash{'TIMESTAMP'} = $tpattern;
    $expecthash{'X-REVISION'} =~ s/\[.*\]/[$tpattern]/;

    foreach ( @$resultref ) {
        ok(/^#([A-Z][A-Z0-9-]*):\s*(.*)$/, '#KEY: val structure');
        my($key, $val) = ($1, $2);
        $val =~ s/\s+$//g;
        if ( ok($expecthash{$key}, "unexpected key '$key'!")
          && ok($val =~ m!^$expecthash{$key}$!, "expected value for key $key") ) {
            $hexpected{$key} = quotemeta($val)};
        delete $expecthash{$key};
      }
    my @excess = keys %expecthash;
    is("", "@excess", 'fields not delivered');
  };

#beacon
subtest "beacon with REVISIT" => sub {
    plan tests => 43;

    my %bexpected = (   # count column is the number of sequences which contain the identifier!
      '118784226' => [2],
      '132464462' => [undef],    # 1 optimized away!
      '118624458' => [undef],
      '103117741' => [undef],
      '118559796' => [undef],
    );

    # %hexpected was initialized from previous test
    $hexpected{'REVISIT'} = $tpattern;

# beacon file is print'ed to STDOUT, we'll have to capture it
    BLOCK: {
        my $copyfd;
        open($copyfd, ">>&", STDOUT) or die "cannot dup current STDOUT: $!";
        close(STDOUT);
        ok(open(STDOUT, ">", 'beacon.out'), 'capture output to beacon.out');
        my($rowcount, $headerref) = $use->beacon( # cgibase unAPIformatname headers_only {preset}
            "http://beacon.example.com/test/",
            undef,         # defaults to "sources"
            1,
            {
	      'REVISIT' => "1d",
              'INSTITUTION' => "Example Corp, http://www.example.com",
            }
          );
        close(STDOUT);
        ok(open(STDOUT, ">>&", $copyfd), "reopening STDOUT");
        close ($copyfd);
    }

    ok(open(SLURP, "<", 'beacon.out'), 'read result back');
    my $inheader = 1;
    my $inbody = 0;
    while ( <SLURP> ) {
        chomp;
        next if /^$/;
        if ( /^#/ ) {
            ok(/^#([A-Z][A-Z0-9-]*):\s*(.*)$/, '#KEY: val structure');
            my($key, $val) = ($1, $2);
            $val =~ s/\s+$//g;
            ok($hexpected{$key}, "unexpected key '$key'!")
              && ok($val =~ m!^$hexpected{$key}$!, "expected value for key $key");
            delete $hexpected{$key};
            $inheader++ if $inheader;
            next;
          }
        unless ( $inbody ) {
            is(--$inheader, 9, 'count header lines');
            $inheader = 0;
          };
        $inbody ++;
        my ($key, @fields) = split(/\s*\|\s*/, $_, 2);
        my $testref;
        ok($testref = $bexpected{$key}, "beacon returned the expected identifier $key");
        is($fields[0], $testref->[0], "[beacon $key]:  (hits)");
#       is($fields[1], $testref->[1], "[beacon $key]:  (info)");
#       is($fields[2], $testref->[2], "[beacon $key]:  (target)");
        delete $bexpected{$key};
      };
    close(SLURP);

    my @hexcess = keys %hexpected;
    is("", "@hexcess", "unexpected header lines from beacon export");

    my @bexcess = keys %bexpected;
    is("", "@bexcess", "unprocessed identifiers from beacon export");
    unlink('beacon.out');
  };

