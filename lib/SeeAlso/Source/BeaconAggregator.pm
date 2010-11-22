package SeeAlso::Source::BeaconAggregator;
use strict;
use warnings;

BEGIN {
    use Exporter ();
    use vars qw($VERSION @ISA @EXPORT @EXPORT_OK %EXPORT_TAGS);
    $VERSION     = '0.1';
    @ISA         = qw(Exporter);
    #Give a hoot don't pollute, do not export more than needed by default
    @EXPORT      = qw();
    @EXPORT_OK   = qw();
    %EXPORT_TAGS = ();
}

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
        "ShortName" => "TestServic",                                # 16 Characters
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

This Module allows a collection of BEACON files (cf. http://de.wikipedia.org/wiki/Wikipedia:PND/BEACON)
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

  $osd_value = SeeAlso::Source::BeaconAggregator->beaconfields('AdultContent');

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
  return $self->SUPER::description(@_);
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
  else {
      $self->{description} = \%result};

  $self->{descriptioncached} = 1;
}

### Antworten fuer Anfragen als Format seealso

=head3 	query( [ $identifier] )

Returns a SeeAlso::Response listing all matches to the given string or
SeeAlso::Identifier $identifier.

=cut

sub query {          # SeeAlso-Simple response
  my ($self, $query) = @_;
  my ($hash, $pretty, $canon) = $self->prepare_query($query);
  my $response = SeeAlso::Response->new($canon);    

  my ($tfield, $afield, $mfield, $m1field, $msfield, $dfield, $nfield, $ifield)
    = map{ scalar $self->beaconfields($_) } 
#        6      7         8       9          10          11          12   13
      qw(TARGET ALTTARGET MESSAGE ONEMESSAGE SOMEMESSAGE DESCRIPTION NAME INSTITUTION);
#              0             1              2              3             4             5
  my ($sql) =<<"XxX";
SELECT beacons.hash, beacons.altid, beacons.seqno, beacons.hits, beacons.info, beacons.link,
       repos.$tfield, repos.$afield, repos.$mfield, repos.$m1field, repos.$msfield, repos.$dfield, repos.$nfield, repos.$ifield
  FROM beacons NATURAL LEFT JOIN repos
  WHERE beacons.hash=? 
  ORDER BY repos.sort, repos.alias;
XxX
  my $sth = $self->{dbh}->prepare($sql) or croak("Could not prepare $sql: ".$self->{dbh}->errstr);
  $sth->execute($hash) or croak("Could not execute $sql: ".$sth->errstr);
  my %didalready;
  while ( my @onerow = $sth->fetchrow_array ) {
      my $hits = $onerow[3];

      my $uri;
      if ( $uri = $onerow[5] ) {                # Expliziter Link
        }
      elsif ( $onerow[1] && $onerow[7] ) {      # Konkordanzformat
          $uri = sprintf($onerow[7], $pretty, urlpseudoescape($onerow[1]))}
      elsif ( $onerow[6] ) {                    # normales Beacon-Format
          $uri = sprintf($onerow[6], $pretty)};
      next unless $uri;

      my $label =  $onerow[8] || $onerow[11] || $onerow[12] || $onerow[13] || "???";
      if ( $hits == 1 ) {
          $label = $onerow[9] if $onerow[9]}
      elsif ( $hits == 0 ) {
          $label = $onerow[10] if $onerow[10]}
      elsif ( $hits ) {
          ($label .= " (%s)") unless ($label =~ /(^|[^%])%s/)};

      $label .= " [".$onerow[4]."]" if $onerow[4];
      $label = sprintf($label, $hits);

#     my $description = $hits;     # entsprechend opensearchsuggestions: pleonastisch, langweilig
#     my $description = $onerow[12] || $onerow[13] || $onerow[8] || $onerow[10] || $onerow[5]; # NAME or INSTITUTION or SOMEMESSAGE or MESSAGE
      my $description = $onerow[13] || $onerow[12] || $onerow[8] || $onerow[10] || $onerow[5]; # INSTITUTION or NAME or SOMEMESSAGE or MESSAGE

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

  my $sql =<<"XxX";
SELECT seqno FROM repos $constraint ORDER BY seqno;
XxX
  my $sth = $self->{dbh}->prepare($sql) or croak("Could not prepare $sql: ".$self->{dbh}->errstr);
  my $aryref = $self->{dbh}->selectcol_arrayref($sth, {Columns=>[1]}, ($query ? ($query) : ()))  or croak("Could not execute $sql: ".$sth->errstr);
  return $aryref ? (@$aryref) : ();
}


=head3 RepoCols ( [ $colname [, $seqno_or_alias ]] ) 

Return all values of column (header field) $colname [alias] 
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
  my $sql =<<"XxX";
SELECT seqno, $dbcolname FROM repos $constraint ORDER BY alias;
XxX
  my $sth = $self->{dbh}->prepare($sql) or croak("Could not prepare $sql: ".$self->{dbh}->errstr);
  my $aryref = $self->{dbh}->selectcol_arrayref($sth, {Columns=>[1..2]}, @cval) or croak("Could not execute $sql: ".$sth->errstr);
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

Returns a hashref containing the OpenSearchDescription keywords an their
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

  my $sql =<<"XxX";
SELECT key, val FROM osd $constraint;
XxX
  my $sth = $self->{dbh}->prepare($sql) or croak("Could not prepare $sql: ".$self->{dbh}->errstr);
  $sth->execute(($key ? ($key) : ())) or croak("Could not execute $sql: ".$sth->errstr);

  my %result = ();
  while ( my @ary = $sth->fetchrow_array ) {
      my ($key, $val) = @ary;
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


=head2 findExample ( $goal, $offset, [ $sth ])

Returns a hashref

 {       id => identier,
   response => Number of beacon files matching "/" Sum of individual hit counts
 }

for the $offset'th identifier with at least $goal hits.

$sth will be initialized by a statement handle to pass to subsequent calls if
defined but false.

=cut

sub findExample {
  my ($self, $goal, $offset, $sth) = @_;
  unless ( $sth ) {
      my ($sql) =<<"XxX";
SELECT hash, COUNT(*), SUM(hits) FROM beacons GROUP BY hash HAVING COUNT(*)>=? LIMIT 1 OFFSET ?;
XxX
#
      $sth = $self->{dbh}->prepare($sql) or croak("Could not prepare $sql: ".$self->{dbh}->errstr);
      $_[3] = $sth if defined $_[3];
    };
  $sth->bind_param(1, $goal, SQL_INTEGER);
  $sth->bind_param(2, $offset, SQL_INTEGER);
  $sth->execute() or croak("Could not execute canned sql (findExampe): ".$sth->errstr);
  if ( my @onerow = $sth->fetchrow_array ) {
      if ( defined $self->{identifierClass} ) {
	  my $c = $self->{identifierClass};
          $c->hash($onerow[0]);
          my $expanded = $c->can("pretty") ? $c->pretty() : $c->value();
          return {id=>$expanded, response=>"$onerow[1]/$onerow[2]"};
        }
      else {
          return {id=>$onerow[0], response=>"$onerow[1]/$onerow[2]"}};
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
sub urlpseudoescape {     # we don't do a thorough job here, because it its not clear whether 
                          # /a/b/c is a parameter ("/" must be encoded) or part of a path ("/" must not be encoded)
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


=head1 BUGS



=head1 SUPPORT

Send mail to the author

=head1 AUTHOR

    Thomas Berger
    CPAN ID: THB
    gymel.com
    THB@cpan.org

=head1 COPYRIGHT

This program is free software; you can redistribute
it and/or modify it under the same terms as Perl itself.

The full text of the license can be found in the
LICENSE file included with this module.


=head1 SEE ALSO

perl(1).

=cut

#################### main pod documentation end ###################

package SeeAlso::Identifier::GND;

sub hash {
  my $self = shift @_;
  if ( @_ ) {
      $self->value(@_)}
  return $self->indexed();
}

sub canonical {
  my $self = shift @_;
  return $self->normalized(@_);
}

1;
