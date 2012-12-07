package SeeAlso::Source::BeaconAggregator;
use strict;
use warnings;

BEGIN {
    use Exporter ();
    use vars qw($VERSION @ISA @EXPORT @EXPORT_OK %EXPORT_TAGS);
    $VERSION     = '0.2_76';
    @ISA         = qw(Exporter);
    #Give a hoot don't pollute, do not export more than needed by default
    @EXPORT      = qw();
    @EXPORT_OK   = qw();
    %EXPORT_TAGS = ();
}

use vars qw($DATA_VERSION);
$DATA_VERSION = 2;

use SeeAlso::Response;
use base ("SeeAlso::Source");

use DBI qw(:sql_types);
use HTTP::Date;

use CGI;
use Carp;

#################### main pod documentation begin ###################
## Below is the stub of documentation for your module. 
## You better edit it!

=head1 NAME

SeeAlso::Source::BeaconAggregator - Beacon files as source for SeeAlso::Server

=head1 SYNOPSIS

  use CGI;
  use SeeAlso::Identifier::ISSN;
  use SeeAlso::Server;
  use SeeAlso::Source::BeaconAggregator;

  my $srcdescription = {
        "ShortName" => "TestService",                               # 16 Characters
         "LongName" => "Sample SeeAlso Beacon Aggregator",          # 48 characters
#     "Description" => "The following services are contained: ...", # 1024 Characters
      "DateModfied" => "...",
    _dont_advertise => 1,
  };

  my $CGI = CGI->new(); binmode(STDOUT, ":utf8");

  my $source = SeeAlso::Source::BeaconAggregator->new(
            'file' => "/path/to/existing/database",
 'identifierClass' => SeeAlso::Identifier::ISSN->new(),
         'verbose' => 1,
     'description' => $srcdescription,
  );

  my $server = SeeAlso::Server->new (
        'cgi' => $CGI,
         xslt => "/client/showservice.xsl",    # => <?xml-stylesheet?> + <?seealso-query-base?>
   clientbase => "/client/",      # => <?seealso-client-base?>
      expires => "+2d",
  );

  my $rawid = $CGI->param('id') || "";
  my $identifier = $rawid ? SeeAlso::Identifier::ISSN->new($rawid) : "";
  my $result = $server->query($source, $identifier ? $identifier->value() : undef);
  print $result;


=head1 DESCRIPTION

This Module allows a collection of BEACON files (cf. http://de.wikipedia.org/wiki/Wikipedia:BEACON)
to be used as SeeAlso::Source (probably in the context of an SeeAlso::Server application).
Therefore it implements the four methods documented in SeeAlso::Source

The BEACON files (lists of non-local identifiers of a certain type documenting the coverage of a given 
online database plus means for access) are imported by the methods provided by 
SeeAlso::Source::BeaconAggregator::Maintenance.pm, usually by employing the script sasbactrl.pl
as command line client.

Serving other formats than SeeAlso or providing a BEACON file with respect to this
SeeAlso service is achieved by using SeeAlso::Source::BeaconAggregator::Publisher.


=head1 USAGE


=head2 Class methods

=cut

our %BeaconFields = (        # in den BEACON-Formaten definierte Felder
                 FORMAT => ['VARCHAR(16)', 1],    # Pflicht
                 TARGET => ['VARCHAR(1024)', 1],  # Pflicht, enthaelt {ID}
# PND-BEACON
                VERSION => ['VARCHAR(16)'],       # Only V0.1 supported
                   FEED => ['VARCHAR(255)'],
                CONTACT => ['VARCHAR(63)'],
            INSTITUTION => ['VARCHAR(1024)'],
                   ISIL => ['VARCHAR(64)'],
            DESCRIPTION => ['VARCHAR(2048)'],
                 UPDATE => ['VARCHAR(63)'],
              TIMESTAMP => ['INTEGER'],
                REVISIT => ['INTEGER'],
# BEACON
               EXAMPLES => ['VARCHAR(255)'],
                MESSAGE => ['VARCHAR(255)'],    # enthaelt {hits}
             ONEMESSAGE => ['VARCHAR(255)'],
            SOMEMESSAGE => ['VARCHAR(255)'],
                 PREFIX => ['VARCHAR(255)'],
# NEWER
                  COUNT => ['VARCHAR(255)'],
                 REMARK => ['VARCHAR(2048)'],
# WInofficial
                   NAME => ['VARCHAR(255)'],
# Experimental
              ALTTARGET => ['VARCHAR(1024)'],
              IMGTARGET => ['VARCHAR(1024)'],
  );



=head3 beaconfields ( [ $what ] ) 

(Class method) Called without parameter returns an array of all valid field names 
for meta headers

  @meta_supported = SeeAlso::Source::BeaconAggregator->beaconfields();

With given parameter $what in scalar context returns the column
name of the database for the abstract field name. In array context
additionally the column type and optional flag designating a 
mandatory entry are returned. 

  $internal_col = SeeAlso::Source::BeaconAggregator->beaconfields('FORMAT');

  ($internal_col, $specs, $mandatory)
      = SeeAlso::Source::BeaconAggregator->beaconfields('FORMAT');

Fields are:

  # mandatory
 FORMAT, TARGET
  # as of BEACON spec
 VERSION, FEED, TIMESTAMP, REVISIT, UPDATE
 CONTACT, INSTITUTION, ISIL, 
  # from the experimental BEACON spec
 MESSAGE, ONEMESSAGE, SOMEMESSAGE
 PREFIX, EXAMPLES
  # later additions
 COUNT, REMARK
  # current practise
 NAME
  # experimental extension "Konkordanzformat"
 ALTTARGET, IMGTARGET


=cut

sub beaconfields {
  my ($class, $what) = @_;
  return keys %BeaconFields unless $what;
  return undef unless $BeaconFields{$what};
  return wantarray ? ("bc$what", @{$BeaconFields{$what}}) : "bc$what";
}


our %OSDElements = (          # fuer OpensearchDescription deklarierte Felder
         "ShortName" => "*", # <= 16 Zeichen, PFLICHT!
       "Description" => "*", # <= 1024 Zeichen, PFLICHT!

           "Contact" => "*", # "nackte" Mailadresse user@domain, optional.
              "Tags" => "*", # Liste von Einzelworten, <= 256 Zeichen, optional.
          "LongName" => "*", # <= 48 Zeichen, optional.
         "Developer" => "*", # <= 64 Zeichen, optional.
       "Attribution" => "*", # <= 256 Zeichen, optional.
  "SyndicationRight" => "open", # open, limited, private, closed
      "AdultContent" => "false", # false/no/0: false, sonst: true

          "Language" => "*",
     "InputEncoding" => "UTF-8",
    "OutputEncoding" => "UTF-8",
#  "dcterms:modified" => "",
# repeatable fields w/o contents, treated specially
#              "Url" => {type => "*", template => "*"},
#            "Query" => {role => "example", searchTerms => "*"},
# Special for the SeeAlso::Family
          "Example" => "*",
         "Examples" => "*",
          "BaseURL" => "*",   # Auto
     "DateModified" => "*",   # alias for dcterms:modified
           "Source" => "*",
  );


=head3 osdKeys ( [ $what ] )

(Class method) Called without parameter returns an array of all valid element names 
for the OpenSearchDescription:

  @meta_names = SeeAlso::Source::BeaconAggregator->osdKeys();

With given parameter $what returns the value for the given OpenSearchDescription
element:

  $osd_value = SeeAlso::Source::BeaconAggregator->beaconfields('LongName');

OSD elements are

 ShortName, Description
 Contact, Tags, LongName, Developer, Attribution, SyndicationRight, AdultContent
 Language, InputEncoding, OutputEncoding
  # special for SeeAlso::Family
 Example, Examples, BaseURL, DateModified, Source

=cut

sub osdKeys {
  my ($class, $what) = @_;
  return keys %OSDElements unless $what;
  return undef unless $OSDElements{$what};
  return $OSDElements{$what};
}


=head2 SeeAlso::Source methods

=head3 new( %accessor [, %options ] )

Creates the SeeAlso::Source::BeaconAggregator object and connects to an existing
database previously created with the methods from 
SeeAlso::Source::BeaconAggregator::Maintenance (currently SQLlite)

Accessor options:

=over 8

=item dbh

handle of a database already connected to

=item dbroot

optional path to prepend to dsn or file

=item dsn

directory name (directory contains the database file "<dsn>-db"

=item file

full path of the database

=back

Other options:

=over 8

=item identifierClass

contains an already instantiated object of that class

=item verbose (0|1)

=item description

Hashref with options to be piped through to SeeAlso::Source

=item aliasfilter

Hashref with aliases to be filtered out from query results

=item cluster

dsn of a beacon source of identical identifier type giving a mapping (hash / altid)
e.g. invalidated identifiers -> current identifiers.

When the identifier supplied for query() is mentioned in this table, the query will be
executed against the associated current identifier and all invalidated ones
(backward translation of forward translation).

When not (the mapping might not necessarily include the identiy mapping), 
the query behaves as if no "cluster" was given.

For translation between different identifier schemes before querying,
use an appropriate SeeAlso::Identifier class.


=back

Returns undef if unable to DBI->connect() to the database.

=cut

sub new {
  my ($class, %options) = @_;
  my $self = {%options};
  bless($self, $class);

  if ( $self->{dsn} ) {
      croak("no special characters allowed for dsn") unless $self->{dsn} =~ /^[\w!,.{}-]+$/};

  if ( $self->{dbroot} ) {
      return undef unless -d $self->{dbroot};
      $self->{dbroot} .= "/" unless $self->{dbroot} =~ m!/$!;
    };

  my $dbfile;
  if ( $self->{dbh} ) {      # called with handle...
      return $self;
    }
  elsif ( $self->{dsn} ) {
      $dbfile = $self->{dsn}."/".$self->{dsn}."-db";
      (substr($dbfile, 0, 0) = $self->{dbroot}) if $self->{dbroot};
    }
  elsif ( $dbfile = $self->{file} ) {
      if ( $self->{dbroot} ) {
          substr($dbfile, 0, 0) = $self->{dbroot}};
    };

  return undef unless $dbfile;

  my $dbh = DBI->connect("dbi:SQLite:dbname=$dbfile", "", "",
          {
#             RaiseError => 1,
              sqlite_unicode => 1,
          });
  return undef unless $dbh;
  $self->{dbh} = $dbh;

  if ( $self->{cluster} ) {
      my $clusterfile = $self->{cluster}."/".$self->{cluster}."-db";
      (substr($clusterfile, 0, 0) = $self->{dbroot}) if $self->{dbroot};
      $dbh->do("ATTACH DATABASE '$clusterfile' AS cluster") or croak("error attaching cluster database '$clusterfile'");
    };

  return $self;
}


=head3 description ()

Inherited from SeeAlso::Source.

=cut

sub description {
  my $self = shift;
  $self->enrichdescription() unless $self->{descriptioncached};
  return $self->SUPER::description(@_);
}

=head3 about ()

Inherited from SeeAlso::Source.

=cut

sub about {
  my $self = shift;
  $self->enrichdescription() unless $self->{descriptioncached};
  return $self->SUPER::about(@_);
}


sub enrichdescription {
  my ($self) = @_;
  my $rawref = $self->OSDValues();
  my %result;
  foreach ( keys %$rawref ) {
      next unless $rawref->{$_};
      if ( ref($rawref->{$_}) ) {      # List
          if ( $_ =~ /^Example/ ) {
              my @ary;
              foreach my $item ( @{$rawref->{$_}} ) {
                  next unless $item;
                  my($i, $r) = split(/\s*\|\s*/, $item, 2);
                  next unless $i;
                  if ( $r ) {
                      push(@ary, {'id'=>$i, 'response'=>$r})}
                  else {
                      push(@ary, {'id'=>$i})}
                }
              $result{$_} = \@ary if @ary;
            }
          else {
              $result{$_} = join(";\n", @{$rawref->{$_}})};
        }
      else {          # Scalar
          if ( $_ =~ /^Example/ ) {
              my($i, $r) = split(/\s*\|\s*/, $rawref->{$_}, 2);
              next unless $i;
              if ( $r ) {
                  $result{$_} = [{'id'=>$i, 'response'=>$r}]}
              else {
                  $result{$_} = [{'id'=>$i}]}
            }
          else {
              $result{$_} = $rawref->{$_}};
        }
    };


  if ( $self->{description} ) {
      my %combined = (%result, %{$self->{description}});
      $self->{description} = \%combined;
    }
  elsif ( %result ) {
      $self->{description} = \%result};

  $self->{descriptioncached} = 1;
}

### Antworten fuer Anfragen als Format seealso

=head3 set_aliasfilter ( @aliaslist )

Init the hash with

=cut

sub set_aliasfilter {
  my ($self, @aliaslist) = @_;
  $self->{'aliasfilter'} = { map { ($_, "") } @aliaslist };
  return $self->{'aliasfilter'};
}

=head3 	query( [ $identifier] )

Returns a SeeAlso::Response listing all matches to the given string or
SeeAlso::Identifier $identifier.

=cut

sub query {          # SeeAlso-Simple response
  my ($self, $query) = @_;
  my ($hash, $pretty, $canon) = $self->prepare_query($query);
  my $response = SeeAlso::Response->new($canon);    

  my $clusterid;
  if ( $self->{cluster} ) {
      my ($clusterh, $clusterexpl) = $self->stmtHdl("SELECT beacons.altid FROM cluster.beacons WHERE beacons.hash=? OR beacons.altid=? LIMIT 1;");
      $self->stmtExplain($clusterexpl, $hash, $hash) if $ENV{'DBI_PROFILE'};
      $clusterh->execute($hash, $hash);
      while ( my $onerow = $clusterh->fetchrow_arrayref() ) {
          $clusterid = $onerow->[0];}
    }

  my ($tfield, $afield, $mfield, $m1field, $msfield, $dfield, $nfield, $ifield)
    = map{ scalar $self->beaconfields($_) } 
#        6      7         8       9          10          11          12   13
      qw(TARGET ALTTARGET MESSAGE ONEMESSAGE SOMEMESSAGE DESCRIPTION NAME INSTITUTION);
#              0             1              2              3             4             5
#            14          15
  my ($sth, $sthexpl);
  if ( $clusterid ) {  # query IN cluster (leader id might not exist at LHS, therefore unionize with beacons.hash=$clusterid (!)
      ($sth, $sthexpl) = $self->stmtHdl(<<"XxX");
SELECT beacons.hash, beacons.altid, beacons.seqno, beacons.hits, beacons.info, beacons.link,
       repos.$tfield, repos.$afield, repos.$mfield, repos.$m1field, repos.$msfield, repos.$dfield, repos.$nfield, repos.$ifield,
       repos.sort, repos.alias
  FROM beacons NATURAL LEFT JOIN repos
  WHERE ( (beacons.hash=?)
       OR (beacons.hash IN (SELECT cluster.beacons.hash FROM cluster.beacons WHERE cluster.beacons.altid=?)) )
  ORDER BY repos.sort, repos.alias;
XxX
      $self->stmtExplain($sthexpl, $clusterid, $clusterid) if $ENV{'DBI_PROFILE'};
      $sth->execute($clusterid, $clusterid) or croak("Could not execute >".$sth->{Statement}."<: ".$sth->errstr);
    }
  else {  # simple query
      ($sth, $sthexpl) = $self->stmtHdl(<<"XxX");
SELECT beacons.hash, beacons.altid, beacons.seqno, beacons.hits, beacons.info, beacons.link,
       repos.$tfield, repos.$afield, repos.$mfield, repos.$m1field, repos.$msfield, repos.$dfield, repos.$nfield, repos.$ifield,
       repos.sort, repos.alias
  FROM beacons NATURAL LEFT JOIN repos
  WHERE beacons.hash=?
  ORDER BY repos.sort, repos.alias;
XxX
      $self->stmtExplain($sthexpl, $hash) if $ENV{'DBI_PROFILE'};
      $sth->execute($hash) or croak("Could not execute >".$sth->{Statement}."<: ".$sth->errstr);
    }

  my $c = $self->{identifierClass} || undef;
  my %didalready;
  while ( my $onerow = $sth->fetchrow_arrayref() ) {
#      last unless defined $onerow->[0];           # strange end condition
      next if $onerow->[15] && exists $self->{'aliasfilter'}->{$onerow->[15]};

      my $hits = $onerow->[3];

      my $h = $onerow->[0];
      my $p;
      if ( $h eq $hash ) {
          $p = $pretty}
      elsif ( $clusterid && ref($c) ) {
          $c->value("");
          my $did = $c->hash($h) || $c->value($h) || $h;
          $p = $c->can("pretty") ? $c->pretty() : $c->value();
        };
      $p = ($clusterid ? $h : $pretty) unless defined $p;

      my $uri;
      if ( $uri = $onerow->[5] ) {                # Expliziter Link
        }
      elsif ( $onerow->[1] && $onerow->[7] ) {    # Konkordanzformat
          $uri = sprintf($onerow->[7], $p, urlpseudoescape($onerow->[1]))}
      elsif ( $onerow->[6] ) {                    # normales Beacon-Format
          $uri = sprintf($onerow->[6], $p)};
      next unless $uri;

      my $label =  $onerow->[8] || $onerow->[11] || $onerow->[12] || $onerow->[13] || "???";
      if ( $hits == 1 ) {
          $label = $onerow->[9] if $onerow->[9]}
      elsif ( $hits == 0 ) {
          $label = $onerow->[10] if $onerow->[10]}
      elsif ( $hits ) {
          ($label .= " (%s)") unless ($label =~ /(^|[^%])%s/)};

      $label .= " [".$onerow->[4]."]" if $onerow->[4];
      $label = sprintf($label, $hits);

#     my $description = $hits;     # entsprechend opensearchsuggestions: pleonastisch, langweilig
#     my $description = $onerow->[12] || $onerow->[13] || $onerow->[8] || $onerow->[10] || $onerow->[5]; # NAME or INSTITUTION or SOMEMESSAGE or MESSAGE
      my $description = $onerow->[13] || $onerow->[12] || $onerow->[8] || $onerow->[10] || $onerow->[5] || ""; # INSTITUTION or NAME or SOMEMESSAGE or MESSAGE

      $response->add($label, $description, $uri) unless $didalready{join("\x7f", $label, $description, $uri)}++;
    }

  return $response;
}

sub prepare_query {
  my ($self, $query) = @_;
  my ($hash, $pretty, $canon);
# search by: $hash
# forward by: $pretty
# normalize by: $canon
  my $c = $self->{identifierClass};
  if ( defined $c ) {   # cast!
        my $qval = ref($query) ? $query->as_string : $query;
        $c->value($qval);
        $hash = $c->hash();
        $pretty = $c->can("pretty") ? $c->pretty() : $c->value();
        $canon = $c->can("canonical") ? $c->canonical() : $c->value();
    }
  elsif ( ref($query) ) {
        $hash = $query->hash();
        $pretty = $query->can("pretty") ? $query->pretty() : $query->value();
        $canon = $query->can("canonical") ? $query->canonical() : $query->value();
    }
  else {
        $hash = $pretty = $canon = $query};

  return ($hash, $pretty, $canon);
}


###

=head2 Auxiliary Methods

Sequence numbers (Seqnos) are primary keys to the database table where
each row contains the meta fields of one BEACON file 


=head3 Seqnos ( $colname , $query )

Return Seqnos from querying the table with all beacon headers in 
column (field name) $colname for a $query 
(which may contain SQL placeholders '%').

=cut

sub Seqnos {
  my ($self, $colname, $query) = @_;

  $colname ||= "";
  $query ||= "";

  my $constraint = "";
  if ( $query ) {
      my $dbcolname = "";
      if ( $colname =~ /^_(\w+)$/ ) {
          $dbcolname = $1}
      elsif ( $dbcolname = $self->beaconfields($colname) ) {}
      else {
          croak("column name '$colname' not known. Aborting")};

      $constraint = ($query =~ /%/) ? "WHERE $dbcolname LIKE ?"
                                    : "WHERE $dbcolname=?";
    };

  my $sth = $self->stmtHdl(<<"XxX");
SELECT seqno FROM repos $constraint ORDER BY seqno;
XxX
  my $aryref = $self->{dbh}->selectcol_arrayref($sth, {Columns=>[1]}, ($query ? ($query) : ()))
      or croak("Could not execute >".$sth->{Statement}."<: ".$sth->errstr);
  return $aryref ? (@$aryref) : ();
}


=head3 RepoCols ( [ $colname [, $seqno_or_alias ]] ) 

Return a hashref indexed by seqence number of all values of column (header field) $colname [alias] 
optionally constrained by a SeqNo or Alias.

Default for $colname is '_alias'.

=cut


sub RepoCols {
  my ($self, $colname, $seqno_or_alias) = @_;
  $colname ||= "_alias";
  $seqno_or_alias ||= "";

  my $dbcolname = "";
  if ( $colname =~ /^_(\w+)$/ ) {
      $dbcolname = $1}
  elsif ( $dbcolname = $self->beaconfields($colname) ) {}
  else {
      croak("column name '$colname' not known. Aborting")};

  my ($constraint, @cval) = mkConstraint($seqno_or_alias);
  my $sth = $self->stmtHdl(<<"XxX");
SELECT seqno, $dbcolname FROM repos $constraint ORDER BY alias;
XxX
  my $aryref = $self->{dbh}->selectcol_arrayref($sth, {Columns=>[1..2]}, @cval)
      or croak("Could not execute >".$sth->{Statement}."<: ".$sth->errstr);
  if ( $aryref ) {
      my %hash = @$aryref;
      return \%hash;
    };
  return undef;
}

sub mkConstraint {
  local ($_) = @_;
  return ("", ()) unless defined $_;
  if ( /^%*$/ )     { return ("", ()) }
  elsif ( /^\d+$/ ) { return (" WHERE seqno=?", $_) }
  elsif ( /%/ )     { return (" WHERE alias LIKE ?", $_) }
  elsif ( $_  )     { return (" WHERE alias=?", $_) }
  else              { return ("", ()) };
}

=head3 OSDValues ( [ $key ] ) 

Returns a hashref containing the OpenSearchDescription keywords and their
respective values.

=cut

sub OSDValues {
  my ($self, $key) = @_;
  $key ||= "";

  my $constraint = "";
  if ( $key =~ /%/ ) {
      $constraint = " WHERE (key LIKE ?)"}
  elsif ( $key ) {
      $constraint = " WHERE (key=?)"};

  my ($sth, $sthexpl) = $self->stmtHdl(<<"XxX");
SELECT key, val FROM osd $constraint;
XxX
  $self->stmtExplain($sthexpl, ($key ? ($key) : ())) if $ENV{'DBI_PROFILE'};
  $sth->execute(($key ? ($key) : ())) or croak("Could not execute >".$sth->{Statement}."<: ".$sth->errstr);

  my %result = ();
  while ( my $aryref = $sth->fetchrow_arrayref ) {
      my ($key, $val) = @$aryref;
#     last unless defined $key;     # undef on first call if nothing to be delivered?
      next if $key =~ /^bc/;        # BeaconMeta Fields smuggled in
      if ( exists $result{$key} ) {
          if ( ref($result{$key}) ) {
              push(@{$result{$key}}, $val)}
          else {
              $result{$key} = [$result{$key}, $val]};
        }
      elsif ( $key eq "DateModified" ) {
          $result{$key} = tToISO($val)}
      else {
          $result{$key} = $val};
    };
  return undef unless %result;
  return \%result;
}

=head3 admhash ( ) 

Returns a hashref with the contents of the admin table (readonly, not tied).

=cut

sub admhash {
  my $self = shift;

  my ($admh, $admexpl) =  $self->stmtHdl("SELECT key, val FROM admin;")
          or croak("Could not prepare statement (dump admin table)".$self->{dbh}->errstr);
  $self->stmtExplain($admexpl) if $ENV{'DBI_PROFILE'};
  $admh->execute() or croak("Could not execute statement (dump admin table): ".$admh->errstr);
  my %adm = ();
  while ( my $onerow = $admh->fetchrow_arrayref() ) {
      if ( $admh->err ) {
          croak("Could not iterate through admin table: ".$admh->errstr)};
      my ($key, $val) = @$onerow;
      $adm{$key} = (defined $val) ? $val : "";
    };
  return \%adm;
}


=head3 autoIdentifier () 

Initializes a missing C<identifierClass> from the IDENTIFIER_CLASS entry in the admin table.

=cut

sub autoIdentifier {
  my ($self) = @_;  

  return $self->{identifierClass} if exists $self->{identifierClass} && ref($self->{identifierClass});

  my ($admich, $admichexpl) =  $self->stmtHdl("SELECT key, val FROM admin WHERE key=?;")
          or croak("Could not prepare statement (dump admin table)".$self->{dbh}->errstr);
  $self->stmtExplain($admichexpl, 'IDENTIFIER_CLASS') if $ENV{'DBI_PROFILE'};
  $admich->execute('IDENTIFIER_CLASS') or croak("Could not execute statement (IDENTIFIER_CLASS from admin table): ".$admich->errstr);
  my %adm = ();
  while ( my $onerow = $admich->fetchrow_arrayref() ) {
      if ( $admich->err ) {
          croak("Could not iterate through admin table: ".$admich->errstr)};
      my ($key, $val) = @$onerow;
      $adm{$key} = $val || "";
    };

  if ( my $package = $adm{"IDENTIFIER_CLASS"} ) {
      eval { $self->{identifierClass} = $package->new() };
      return $self->{identifierClass} unless $@;

      eval {
          (my $pkgpath = $package) =~ s=::=/=g;  # require needs path...
          require "$pkgpath.pm";
          import $package;
        };
      if ( $@ ) {
         croak "sorry: Identifier Class $package cannot be imported\n$@"};

      return $self->{identifierClass} = $package->new();
    };
  return undef;
}


=head3 findExample ( $goal, $offset, [ $sth ])

Returns a hashref

 {       id => identier,
   response => Number of beacon files matching "/" Sum of individual hit counts
 }

for the C<$offset>'th identifier occuring in at least C<$goal> beacon instances.

$sth will be initialized by a statement handle to pass to subsequent calls if
defined but false.

=cut

sub findExample {
  my ($self, $goal, $offset, $sth) = @_;
  my $sthexpl;
  unless ( $sth ) {
      ($sth, $sthexpl) = $self->stmtHdl(<<"XxX");
SELECT hash, COUNT(*), SUM(hits) FROM beacons GROUP BY hash HAVING COUNT(*)>=? LIMIT 1 OFFSET ?;
XxX
#
      $_[3] = $sth if defined $_[3];
    };
  $offset ||= 0;
  $sth->bind_param(1, $goal, SQL_INTEGER);
  $sth->bind_param(2, $offset, SQL_INTEGER);
  if ( $sthexpl && $ENV{'DBI_PROFILE'} ) {
      $sthexpl->[0]->bind_param(1, $goal, SQL_INTEGER);
      $sthexpl->[0]->bind_param(2, $offset, SQL_INTEGER);
      $self->stmtExplain($sthexpl);
    };
  $sth->execute() or croak("Could not execute canned sql (findExample): ".$sth->errstr);
  if ( my $onerow = $sth->fetchrow_arrayref ) {
      if ( defined $self->{identifierClass} ) {
	  my $c = $self->{identifierClass};
# compat: hash might not take an argument, must resort to value, has to be cleared before...
          $c->value("");
          my $did = $c->hash($onerow->[0]) || $c->value($onerow->[0]);
          my $expanded = $c->can("pretty") ? $c->pretty() : $c->value();
          return {id=>$expanded, response=>"$onerow->[1]/$onerow->[2]"};
        }
      else {
          return {id=>$onerow->[0], response=>"$onerow->[1]/$onerow->[2]"}};
    };
  return undef;
};

# Date prettyprint

sub tToISO {
  local($_) = HTTP::Date::time2isoz($_[0] || 0);
  tr[ ][T];
  return $_;
}

# URL-encode data
sub urlpseudoescape {     # we don't do a thorough job here, because it is not clear whether 
                          # /a/b/c is a parameter ("/" must be encoded) or part of a path ("/" must not be encoded)
                          # and we must avoid  URL-escaping already escaped content
                          # Therefore we only escape spaces and characters > 127
  local ($_) = @_;
  $_ = pack("C0a*", $_);  # Zeichen in Bytes zwingen
  # FYI
  # reserved uri characters: [;/?:@&=+$,] by RFC 3986
  # ;=%3B  /=%2F  ?=%3F  :=%3A  @=%40  &=%26  ==%3D  +=%2B  $=%24  ,=%2C
  # delims = [<>#%"], unwise =  [{}|\\\^\[\]`]
  # mark (nreserved) = [-_.!~*'()]
  #                     222222257
  #                     1789ACEFE
#      s/([^a-zA-Z0-9!'()*\-._~])/sprintf("%%%02X",ord($1))/eg;
      s/([^\x21-\x7e])/sprintf("%%%02X",ord($1))/eg;
  return $_;
}


# SQL handle management
sub stmtHdl {
  my ($self, $sql, $errtext) = @_;
  $errtext ||= $sql;
  my $if_active = $ENV{'DBI_PROFILE'} ? 0 : 1;
  my $sth = $self->{dbh}->prepare_cached($sql, {}, $if_active)
      or croak("Could not prepare $errtext: ".$self->{dbh}->errstr);
  return $sth unless wantarray;
  if ( $ENV{'DBI_PROFILE'} ) {
      my @callerinfo = caller;
      print STDERR "reusing handle for $sql (@callerinfo)===\n" if $sth->{Executed};
      my $esth = $self->{dbh}->prepare_cached("EXPLAIN QUERY PLAN $sql", {}, 0)
          or croak("Could not prepare explain query plan stmt: ".$self->{dbh}->errstr);
      return $sth, [$esth, $sql];
    }
  else {
      return $sth, undef};
};

sub stmtExplain {
  my ($self, $eref, @args) = @_;
  my $esql = $eref->[1];
  my @callerinfo = caller;
  print STDERR "explain $esql\n\tfor data @args\n(@callerinfo)===\n";
  my $esth = $eref->[0];
  $esth->execute(@args) or croak("cannot execute explain statement $esql with args @args");
  local $" = " | ";
  while ( my $rowref = $esth->fetchrow_arrayref ) {
       print STDERR "@$rowref\n";
    }
  print STDERR "===\n";
}


=head1 BUGS



=head1 SUPPORT

Send mail to the author

=head1 AUTHOR

Thomas Berger <ThB@gymel.com>

=head1 COPYRIGHT

This program is free software; you can redistribute
it and/or modify it under the same terms as Perl itself.

The full text of the license can be found in the
LICENSE file included with this module.


=head1 SEE ALSO

perl(1).

=cut

#################### main pod documentation end ###################

1;

