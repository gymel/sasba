package SeeAlso::Source::BeaconAggregator::Publisher;
use strict;
use warnings;

our $VERSION = "0.1";

=head1 NAME

SeeAlso::Source::BeaconAggregator::Publisher - additional methods for SeeAlso::Source::BeaconAggregator

=head1 SYNOPSIS

  $source = SeeAlso::Source::BeaconAggregator::Publisher->new(...);

=head1 DESCRIPTION

This package provides the functionallity to export a BEACON file from the
data connected with an SeeAlso::Source::BeaconAggregator instance and
also the additional formats "redirect" and "sources" which universally
can be used as callbacks for SeeAlso::Server (replacing the default
"seealso" method yielding JSON data).

=cut

our %Defaults = (
    "REVISIT" => 86400,             # one day
    "uAformatname" => "sources",
    "beaconformatname" => "beacon",
    "FORMAT"  => "BEACON",
    "VERSION" => $VERSION,
);

use SeeAlso::Source::BeaconAggregator;
use Carp;

=head2 new ( ... )

Creates an SeeAlso::Source::BeaconAggregator object with additional methods from
this package enabled

=cut

sub new {                 # directly create BeaconAggregator instance with extended features...
  my $class = shift @_;
  push(@SeeAlso::Source::BeaconAggregator::ISA, $class);
  return SeeAlso::Source::BeaconAggregator->new(@_);
}


=head2 activate ()

Makes SeeAlso::Source::BeaconAggregator objects member of this class,
globally enabling the additional methods

Usage:

  $db = SeeAlso::Source::BeaconAggregator::Maintenance->new(...);
  ...
  do stuff
  ...
  require SeeAlso::Source::BeaconAggregator::Publisher
          or die "could not require Publisher extension";
  SeeAlso::Source::BeaconAggregator::Publisher->activate();   # "recast" all objects
  ...
  do more stuff

=cut
sub activate {            # enrich SeeAlso::Source and derived classes with our methods
  my $class = shift @_;
  push(@SeeAlso::Source::BeaconAggregator::ISA, $class);
  return 1;
}


### Produktion der Beacon-Datei

=head2 beacon ( [dumpmeta arguments] )

produces a BEACON file (however, $cgibase is mandatory)

=head2 dumpmeta ( [$cgibase, [$uAformatname, [$headersonly]]] [, $preset])

produces only the meta fields of a BEACON file 

Meta fields are generated from the $preset Hashref, falling back to 
values stored in the database, falling back to reasonable default
values.

Arguments:

=over 8

=item $cgibase

URL of the SeeAlso service the BEACON file is provided for

=item $uAformatname

unAPI format name to be used as target (Default: "sources")

=item $headersonly

currently unused

=item $preset

Hashref of Beacon header fields overriding the contents of the database 

=back

Regular Usage:

  $db = SeeAlso::Source::BeaconAggregator::Publisher->new(...);
  binmode(STDOUT, ":utf8");
  my $cgibase = "http://address/of/service";
  my ( $error, $headerref) = $db->beacon($cgibase, @ARGV, {'FORMAT' => 'PND-BEACON'});


CGI Usage:

  $format = $CGI->param('format') || "";
  if ( $format eq "beacon" ) {  # bypass SeeAlso::Server->query() b/c performance / interim storage
                                  insert access restrictions here...
      do_beacon($source, $CGI);
    }
  ...

 sub do_beacon {
   my ($self, $cgi) = @_;           # Of type SeeAlso::Source::BeaconAggregator
   unless ( $self->can("beacon") ) {
       croak "On the fly generation of beacon Files not supported by this service";}
   my $cgibase = $cgi->url(-path_info=>1);

   print $cgi->header( -status => 200,
                      -expires => '+1d',
                         -type => 'text/plain',
                      -charset => 'utf-8',
                      );
   return $self->beacon($cgibase, "sources", {});     # prints directly to stdout..., returns $error, $headerref
 }

=cut

sub beacon {
  my ($self) = shift @_ or croak("beacon is a method!");         # Of type SeeAlso::Source::BeaconAggregator
  my ($error, $headerref) = $self->dumpmeta(@_);
  croak("Error generating Header, will not proceed") if $error;

  print @$headerref;

  my $sql =<<"XxX";
SELECT hash, COUNT(DISTINCT seqno) FROM beacons GROUP BY hash ORDER BY hash;
XxX
  my $sth = $self->{dbh}->prepare($sql) or croak("Could not prepare $sql: ".$self->{dbh}->errstr);
  $sth->execute() or croak("Could not execute $sql: ".$sth->errstr);
  my $c = (defined $self->{identifierClass}) ? $self->{identifierClass} : undef;
  my $rows = 0;
  while ( my $row = $sth->fetchrow_arrayref ) {
      $rows++;
      my $expanded = $row->[0];
      if ( defined $c ) {
          $c->hash($row->[0]);
          $expanded = $c->can("pretty") ? $c->pretty() : $c->value();
        }
      print $expanded.(($row->[1] > 1) ? "|".$row->[1] : "")."\n"}

  return $rows, $headerref;
}

sub dumpmeta {    # cgibase unAPIformatname headers_only {preset}
  my ($self) = shift @_ or croak("dumpmeta is a method!");         # Of type SeeAlso::Source::BeaconAggregator
  my ($error, @result) = (0, ());

  my $cgibase = shift @_ if @_ && !ref($_[0]);
  my $uAformatname = shift @_ if @_ && !ref($_[0]);
  $uAformatname ||= $Defaults{'uAformatname'};
  my $headersonly = shift @_ if @_ && !ref($_[0]);
  my $preset = (@_ && ref($_[0])) ? (shift @_) : {};

  my $metasql =<<"XxX";
SELECT key, val FROM osd;
XxX
  my $metasth = $self->{dbh}->prepare($metasql) or croak("Could not prepare $metasql: ".$self->{dbh}->errstr);
  $metasth->execute() or croak("Could not execute $metasql: ".$metasth->errstr);

  my (%osd, %beaconmeta);
  while ( my @ary = $metasth->fetchrow_array ) {
      my ($key, $val) = @ary;
      next unless $val;
      if ($key =~ s/^bc// ) {        # BeaconMeta Fields
          $beaconmeta{$key} = $val}
      elsif ( exists $osd{$key} ) {
          if ( ref($osd{$key}) ) {
              push(@{$osd{$key}}, $val)}
          else {
              $osd{$key} = [$osd{$key}, $val]};
        }
      else {
          $osd{$key} = $val};
    };
  my @osdexamples;
  if ( $osd{'Examples'} && ref($osd{'Examples'}) ) {
      foreach my $expl ( @{$osd{'Examples'}} ) {
          $expl =~ s/\s*\|.*$//;
          push(@osdexamples, $expl);
        }
    }
  elsif ( my $expl = $osd{'Examples'} ) {
      $expl =~ s/\s*\|.*$//;
      push(@osdexamples, $expl);
    };

# Mandatory fields
  push(@result, "#FORMAT: ".($preset->{'FORMAT'} || $beaconmeta{'FORMAT'} || $Defaults{'FORMAT'})."\n");
  push(@result, "#VERSION: ".($preset->{'VERSION'} || $beaconmeta{'VERSION'} || $Defaults{'VERSION'})."\n");
  if ( $preset->{'TARGET'} ) {
      push(@result, "#TARGET: ".$preset->{'TARGET'}."\n")}
  elsif ( $beaconmeta{'TARGET'} ) {
      push(@result, "#TARGET: $beaconmeta{'TARGET'}\n")}
  elsif ( $cgibase ) {
      push(@result, "#TARGET: $cgibase?format=$uAformatname&id={ID}\n")}
  else {
      carp "Don't know how to construct the mandatory #TARGET field!";
      $error ++;
    }

  my $timestamp = $preset->{'TIMESTAMP'} || $osd{DateModified} || $^T;
  push(@result, "#TIMESTAMP: ".SeeAlso::Source::BeaconAggregator::tToISO($timestamp)."\n") if $timestamp > 0;
  my $revisit = ($preset->{'REVISIT'} || $beaconmeta{'REVISIT'} || $Defaults{'REVISIT'}) || "";
  $revisit =~ tr/ //d;
  $revisit =~ s/(\d+)mo\w*/($1*30)."d"/ei;
  $revisit =~ s/(\d+)M\w*/($1*30)."d"/e;
  $revisit =~ s/(\d+)w\w*/($1*7)."d"/ei;
  $revisit =~ s/(\d+)d\w*/($1*24)."h"/ei;
  $revisit =~ s/(\d+)h\w*/($1*60)."m"/ei;
  $revisit =~ s/(\d+)m\w*/($1*60)."s"/ei;
  $revisit =~ s/(\d+)s\w*/$1/i;
  push(@result, "#REVISIT: ".SeeAlso::Source::BeaconAggregator::tToISO($timestamp + $revisit)."\n") if $revisit && ($revisit =~ /^[+-]?\d+$/) && ($revisit > 0);;

# $beaconmeta{'UPDATE'} ||= "daily";
  $beaconmeta{'FEED'} ||= "$cgibase?format=".$Defaults{'beaconformatname'} if $cgibase;
  $beaconmeta{'EXAMPLES'} ||= join("|", @osdexamples);
  $beaconmeta{'CONTACT'} ||= $self->{Contact} || $osd{'Contact'};
  $beaconmeta{'DESCRIPTION'} ||= $self->{Description} || $osd{'Description'};
  $beaconmeta{'NAME'} ||= $self->{ShortName} || $osd{'ShortName'};
  foreach ( grep !/^(FORMAT|REVISIT|TARGET|TIMESTAMP|VERSION)$/, sort keys %beaconmeta ) {
      next unless my $val = $preset->{_} || $beaconmeta{$_};
      next if $val =~ /^-/;
      $val =~ s/\s+/ /g; $val =~ s/^\s+//; $val =~ s/\s+$//;
      push(@result, "#$_: $val\n");
    }

## PND-BEACON
#                CONTACT => ['VARCHAR(63)'],
#            INSTITUTION => ['VARCHAR(255)'],
#                   ISIL => ['VARCHAR(63)'],
#            DESCRIPTION => ['VARCHAR(255)'],
## BEACON
#                MESSAGE => ['VARCHAR(255)'],    # enthaelt {hits}
#             ONEMESSAGE => ['VARCHAR(255)'],
#            SOMEMESSAGE => ['VARCHAR(255)'],
#                 PREFIX => ['VARCHAR(255)'],
## WInofficial
#                   NAME => ['VARCHAR(255)'],

  return $error, \@result;
}


=head2 redirect ( $server, $format, $extra, $query )

Produces an HTTP redirect page, HTML content contains very terse details in case
of multiple results.

This subroutine may be used as callback method in SeeAlso::Server

Usage is a bit cludgy due to author's lack of understanding of SeeAlso::Server

  $source = SeeAlso::Sources::BeaconAggregator::Publisher->new(...);
  $CGI = CGI->new();

  $formats = {
    ...
    redirect => {
           type => "text/html", 
           docs => "http://www.endofthe.net/",
#        method => \&SeeAlso::Source::BeaconAggregator::Publisher::redirect,
  #redirect_300 => 'sources',
                }
  };

  $server   = SeeAlso::Server->new (
          'cgi' => $CGI,
      'formats' => $formats,
       ...
  );

  # Closure as fix: Server.pm does not expose self, $source and the CGI object to the format methods
  my $oref = \&SeeAlso::Source::BeaconAggregator::Publisher::redirect;
  $server->{'formats'}->{'redirect'}->{method}
    = sub {return &$oref($source, $server, $method, $formats->{$method}, @_)};

  my $result = $server->query($source);

Arguments:

=over 8

=item $server

SeeAlso::Server object. Must contain a CGI object

=item $format

Name of a format registered with the $server object ()

=item $extra

Hashref with the following configuration directives

  redirect_300 => CGI 'format' parameter to be used in HTML content (eg. format=sources)

=item $query

Identifier to be queried

=back

=cut

sub redirect {          # Liste der Beacon-Header fuer Treffer oder einfaches redirect
  my ($self, $server, $format, $extra, $query) = @_;
  my $formatprops = $server->{'formats'}->{$format} || {};
  my $cgi = $server->{'cgi'} or croak("I rely on a prepared CGI.pm object");

  my %headerdefaults = (               -type => ($formatprops->{'type'} || 'text/html'),
#      ($formatprops->{'charset'} ? (-charset =>  $formatprops->{'charset'}) : ()),
                                     -charset => ($formatprops->{'charset'} || 'UTF-8'),
                                    -expires => ($server->{'expires'} || '+1d'),
    );

  my ($hash, $pretty, $canon) = $self->prepare_query($query);
  unless ( $hash ) {
      print $cgi->header(-status => "400 Bad Request (Identifier '$query' not valid)",
                        -expires => "+1y",
                           -type => 'text/html',
                         ),
            $cgi->start_html (-dtd => "-//W3C//DTD HTML 3.2 Final//EN",
                            -title => "No valid identifier",
                              ),
            $cgi->p("Malformed identifier '$query'"),
            $cgi->end_html;
      return "";
    };

  my (  $tfield,$afield,  $gfield,  $mfield,$nfield,$ifield) = map{ scalar $self->beaconfields($_) } 
      qw(TARGET  ALTTARGET IMGTARGET MESSAGE NAME   INSTITUTION);
# above  4       5         6         7       8      9
# below        0              1             2             3
  my $sql =<<"XxX";
SELECT beacons.altid, beacons.hits, beacons.info, beacons.link,
       repos.$tfield, repos.$afield, repos.$gfield, repos.$mfield, repos.$nfield, repos.$ifield,
       repos.alias
  FROM beacons NATURAL LEFT JOIN repos
  WHERE beacons.hash=? 
  ORDER BY repos.sort, repos.alias;
XxX
  my $sth = $self->{dbh}->prepare($sql) or croak("Could not prepare $sql: ".$self->{dbh}->errstr);
  $sth->execute($hash) or croak("Could not execute $sql: ".$sth->errstr);
  my @rawres;
  while ( my @onerow = $sth->fetchrow_array ) {
      my $uri = $onerow[3];         # Evtl. Expliziter Link
      my $guri = "";

      if ( $onerow[0] ) {      # Konkordanzformat
          $uri ||= sprintf($onerow[5] || $onerow[4], $pretty, SeeAlso::Source::BeaconAggregator::urlpseudoescape($onerow[0]));
          $guri = sprintf($onerow[6], $pretty, SeeAlso::Source::BeaconAggregator::urlpseudoescape($onerow[0])) if $onerow[6];
        }
      elsif ( $onerow[4] ) {                    # normales Beacon-Format
          $uri = sprintf($onerow[4], $pretty);
          $guri = sprintf($onerow[6], $pretty) if $onerow[6];
        };
      next unless $uri;

#                       #NAME         #INSTITUTION  _alias
      my $label;
      if ( $label = $onerow[7] ) { #MESSAGE 
          $label = sprintf($label, $onerow[1] || "?")}
      elsif ( $label = $onerow[8] || $onerow[9] || $onerow[10] || "???" ) {
          $label .= " (".$onerow[0].")" if $onerow[0]}

      push(@rawres, [$uri, $guri, $label, $onerow[10], $onerow[2]]);
    };
  my $hits = scalar @rawres;

  if ( ! $hits ) {
      print $cgi->header(-status => "404 Not Found (identifier '$canon')",
                         %headerdefaults),
            $cgi->start_html (-dtd => "-//W3C//DTD HTML 3.2 Final//EN",
                            -title => "No References for $pretty",
                              ),
            $cgi->p("No References found for ", $cgi->a({href=>"$canon"}, $pretty)),
            $cgi->end_html;
      return "";
    }
  elsif ( $hits == 1 ) {
      return $cgi->redirect(-status => "302 Found (Redirecting for identifier '$canon')",
                               -uri => $rawres[0]->[0],
                            %headerdefaults);
    }

  my $sources = new CGI($cgi);
  $sources->param(-name => 'id', -value=>"$canon");
  if ( my $multired = $extra->{redirect_300} ) {
      $sources->param(-name => 'format', -value=>$multired);
      print $cgi->redirect(-status => "300 Multiple Choices for identifier '$canon'",
                              -uri => $sources->url(-path_info=>1, -query=>1),
                           %headerdefaults);
    }
  else {
      print $cgi->header(-status => "300 Multiple Choices for identifier '$canon'",
                         %headerdefaults);
    };
  my @result;
  push(@result, $cgi->start_html ( -title => "$hits References for $pretty",
                                     -dtd => "-//W3C//DTD HTML 3.2 Final//EN"),
                $cgi->h1("$hits References for ", $cgi->a({href=>"$canon"}, $pretty)),
                '<ul>');

  my $rowcnt = 0;
  foreach ( @rawres ) {  # uri, guri, label, alias, info
      if ( $_->[1] ) {
          my $tooltip = $_->[4] ? ($_->[4]." [".$_->[2]."]") : $_->[2];
          my $img =  $cgi->a({href=>$_->[0], title=>$tooltip}, $cgi->img({src=>$_->[1], alt=>$_->[4]||$_->[2], style=>"width: 5em; border: 0pt;"}));
          push(@result, $cgi->li({id=>"$_->[3]".++$rowcnt}, $img, $cgi->a({href=>$_->[0]}, $_->[2]), ($_->[4] ? " [".$_->[4]."]" : "")));
        }
      else {
          push(@result, $cgi->li({id=>"$_->[3]".++$rowcnt}, $cgi->a({href=>$_->[0]}, $_->[2]), $_->[4] ? " [".$_->[4]."]" : ""))};
    };

  push(@result, '</ul>');

  if ( $server->{'formats'}->{'sources'} ) {
      $sources->param(-name => 'format', -value=>"sources");
      push(@result, $cgi->p("[", $cgi->a({href=>($sources->url(-path_info=>1, -query=>1))}, "Details"), "]"));
    };

  my($tu, $ts, $tcu, $tcs) = times();
  push(@result, sprintf("<!-- user: %.3fs + sys: %.3fs = %.3fs -->", $tu, $ts, $tu+$ts), $cgi->end_html());
  return join("\n", @result);
}

=head2 sources ( $server, $format, $extra, $query )

Produces an HTML page with details to the queried identifier (description of sources)

This subroutine may be used as callback method in SeeAlso::Server (cf. description
of redirect above

=over 8

=item $server

SeeAlso::Server object


=item $format

Format selected for $server


=item $extra

Hashref with the following configuration directives

  css => URL of css file to be referenced

=item $query

Identifier to be queried

=back

=cut

sub sources {          # Liste der Beacon-Header fuer Treffer
                       # We escape all characters except US-ASCII, because older CGI.pm's set an xml declaration
                       # which somehow interferes with IE8's adherence to the character set...
  my ($self, $server, $format, $extra, $query) = @_;
  my $formatprops = $server->{'formats'}->{$format} || {};
  my $cgi = $server->{'cgi'} || CGI->new();

  my ($hash, $pretty, $canon) = $self->prepare_query($query);
  unless ( $hash ) {
      print $cgi->header(-status => "400 Bad Request (Identifier '$query' not valid)",
                        -expires => "+1y",
                           -type => 'text/html',
                         ),
            $cgi->start_html (-dtd => "-//W3C//DTD HTML 3.2 Final//EN",
                            -title => "No valid identifier",
                              ),
            $cgi->p("Malformed identifier '$query'"),
            $cgi->end_html;
      return "";
    };

  my ($countsql) =<<"XxX";
SELECT COUNT(DISTINCT seqno) FROM beacons WHERE hash=?;
XxX
  my $countsth = $self->{dbh}->prepare($countsql) or croak("Could not prepare $countsql: ".$self->{dbh}->errstr);
  $countsth->execute($hash) or croak("Could not execute $countsql: ".$countsth->errstr);
  my $hits = $countsth->fetchrow_array || 0;

  my ($osd, $beaconmeta) = $self->get_meta;
  my $prefix = $beaconmeta->{'PREFIX'} || "";
  (my $servicename = $beaconmeta->{'NAME'} || $osd->{'ShortName'} || "") =~ s/([<>&"]|[^\x00-\x7f])/'&#'.ord($1).';'/ge;
  
  my $target = $cgi->url(-path=>1);

  my @result;
  push(@result, $cgi->start_html( -xhtml => 1,
                               -encoding => "UTF-8",
                                  -title => "$servicename referring ".$query->as_string(),
                                   -meta => {'robots'=>'noindex'},
              ($extra->{'css'} ? (-style =>{'src'=>$extra->{'css'}}) : ()),
                                   -head =>[$cgi->Link({-rel=>'unapi-server', -type=>'application/xml', title=>'unAPI', -href=>$target}),
                                            $cgi->Link({-rel=>'start', -href=>$target})],
                                 ));

  push(@result, '<script type="text/javascript">function toggle(divid) {if ( document.getElementById(divid).style.display == "none" ) {document.getElementById(divid).style.display = "block"} else {document.getElementById(divid).style.display = "none"}}</script>');
  push(@result, '<script type="text/javascript">function mtoggle(dlid,cl) {var nd=document.getElementById(dlid).firstChild; while (nd!=null){if (nd.nodeType == 1) {if (nd.className==cl) {if (nd.style.display == "none"){nd.style.display = "block"}else{nd.style.display = "none"}}};nd=nd.nextSibling;};}</script>');

  push(@result, $cgi->h1("$hits References for ".$cgi->abbr({class=>"unapi-id", title=>"$canon"}, $query)));

  push(@result, '<div id="description">');
  push(@result, $cgi->p($cgi->span("Identifier:"), $cgi->a({href=>"$prefix$pretty"}, "$prefix$pretty"))) if $prefix;
  push(@result, '</div>');

  my ($sql) =<<"XxX";
SELECT beacons.*, repos.*
  FROM beacons NATURAL LEFT JOIN repos
  WHERE beacons.hash=? 
  ORDER BY repos.sort, repos.alias;
XxX
  my $sth = $self->{dbh}->prepare($sql) or croak("Could not prepare $sql: ".$self->{dbh}->errstr);
  $sth->execute($hash) or croak("Could not execute $sql: ".$sth->errstr);
  my $rows = 0;
  push(@result, '<div id="results">');
  my ($lastseq, @groups) = (0, ());
  while ( my $onerow = $sth->fetchrow_hashref ) {
      $rows ++;
      if ( $lastseq and $onerow->{'seqno'} == $lastseq ) {
          my %vary;
          foreach my $key ( grep /^(hash|altid|hits|info|link)$/, keys %$onerow ) {
              my $pval = $onerow->{$key};
              next unless defined $pval;
              $pval =~ s/([<>&"]|[^\x00-\x7f])/'&#'.ord($1).';'/ge if $key eq "link";
              $vary{$key} = $pval;
            }
          push(@{$groups[$#groups]}, \%vary);
        }
      else {
          my (%vary, %repos, %meta);
          while ( my($key, $val) = each %$onerow ) {
              my $pval = $val;
              unless ( $key =~ /feed|target|uri|link/i ) {
                  $pval =~ s/([<>&"]|[^\x00-\x7f])/'&#'.ord($1).';'/ge if defined $pval};
              if ( $key =~ /time|revisit/i ) {
                  next unless $val;
                  $pval = HTTP::Date::time2str($val);
                };
              if ( $key =~ /^bc(\w+)$/ ) {
                  $repos{$1} = $pval if $pval}
              elsif ( $key =~ /^(hash|altid|hits|info|link)$/ ) {
                  $vary{$key} = $pval}
              else {
                  $meta{"_$key"} = $pval if $pval}
            };
           push(@groups, [\%repos, \%meta, \%vary]);
         };
      $lastseq = $onerow->{'seqno'};
    };
# Grouping done, now display...

  foreach my $groupref ( @groups ) {
      my ($repos, $meta, @vary) = @$groupref;

      my $aos = $meta->{'_alias'} || $meta->{'_seqno'};

      my $multi = (scalar @vary > 1) ? "multi" : "single";
      push(@result, qq!<div class="result $multi" id="resgrp$aos">!);
      push(@result, $cgi->h3({class=>"aggregator", onClick=>"toggle('ag$aos')"}, "Administrative Details"));

      push(@result, $cgi->h3({class=>"beacon", onClick=>"toggle('bc$aos')"}, "Repository Details"));

      if ( $multi eq "single" ) {
          push(@result, $cgi->h3({class=>"hit", onClick=>"toggle('ht$aos')"}, "Result Details"));

          my $vary = $vary[0];

          my $hits = $vary->{'hits'};
          my $description = $hits;
          my $uri = "???";
          if ( $uri = $vary->{'link'} ) {  # o.k.
            }
          elsif ( $repos->{'ALTTARGET'} && $vary->{'altid'} ) {
              $uri = sprintf($repos->{'ALTTARGET'}, $pretty, SeeAlso::Source::BeaconAggregator::urlpseudoescape($vary->{'altid'}))}
          elsif ( $repos->{'TARGET'} ) {
              $uri = sprintf($repos->{'TARGET'}, $pretty)};

          my $guri = "";
          if ( $repos->{'IMGTARGET'} ) {
              $guri = sprintf($repos->{'IMGTARGET'}, $pretty, SeeAlso::Source::BeaconAggregator::urlpseudoescape($vary->{'altid'}))}

          my $rlabel =  $repos->{'MESSAGE'} || $repos->{'DESCRIPTION'} || $repos->{'NAME'} || $repos->{'INSTITUTION'} || "???";
          if ( $hits == 1 ) {
              $rlabel = $repos->{'ONEMESSAGE'} if $repos->{'ONEMESSAGE'}}
          elsif ( $hits == 0 ) {
              $rlabel = $repos->{'SOMEMESSAGE'} if $repos->{'SOMEMESSAGE'}};
          my $label = sprintf($rlabel, $hits);

          push(@result, $cgi->a({style=>"float: right; clear: right;", href=>$uri}, $cgi->img({alt=>$vary->{'info'}||$label,src=>$guri}))) if $guri;

          push(@result, $cgi->h2({class=>"label", id=>"head$aos"}, $cgi->a({href=>$uri}, $label)));

          push(@result, qq!<div class="synopsis" id="syn$aos">!);
          push(@result, $cgi->span($vary->{'info'})) if $vary->{'info'};
          push(@result, $cgi->span("($hits Treffer)")) if $hits && ($rlabel !~ /%s/);
          push(@result, '</div>');

          push(@result, qq!<div class="hit" id="ht$aos" style="display: none;">!);
          push(@result, $cgi->p({class=>"ht_uri"}, $cgi->span("Target URL:"), $cgi->a({href=>$uri}, CGI::escapeHTML($uri))));
          push(@result, $cgi->p({class=>"ht_guri"}, $cgi->span("Preview URL:"), $cgi->a({href=>$guri}, $guri))) if $guri;
          push(@result, $cgi->p({class=>"ht_hits"}, $cgi->span("Hits:"), $hits)) if $hits;
          push(@result, $cgi->p({class=>"ht_info"}, $cgi->span("Additional Info:"), $vary->{'info'})) if $vary->{'info'};
          push(@result, '</div>');
        }
      else {
          push(@result, $cgi->h3({class=>"hit", onClick=>"mtoggle('res$aos', 'hit')"}, "Result Details"));
          my $hits = scalar @vary;
          my $rlabel =  $repos->{'SOMEMESSAGE'} || $repos->{'MESSAGE'} || $repos->{'DESCRIPTION'} || $repos->{'NAME'} || $repos->{'INSTITUTION'} || "???";
          my $label = sprintf($rlabel, $hits);
          push(@result, $cgi->h2({class=>"label", id=>"head$aos"}, $label));

          push(@result, qq!<dl id="res$aos">!);
          my $cnt = 0;
          foreach my $vary ( @vary ) {
              $cnt ++;

              my $uri = "???";
              if ( $uri = $vary->{'link'} ) {  # o.k.
                }
              elsif ( $repos->{'ALTTARGET'} && $vary->{'altid'} ) {
                  $uri = sprintf($repos->{'ALTTARGET'}, $pretty, SeeAlso::Source::BeaconAggregator::urlpseudoescape($vary->{'altid'}))}
              elsif ( $repos->{'TARGET'} ) {
                  $uri = sprintf($repos->{'TARGET'}, $pretty)};

              my $guri = "";
              if ( $repos->{'IMGTARGET'} ) {
                  $guri = sprintf($repos->{'IMGTARGET'}, $pretty, SeeAlso::Source::BeaconAggregator::urlpseudoescape($vary->{'altid'}))}

              my $hits = $vary->{hits} if $vary->{hits} and $vary->{hits} != 1;


              push(@result, qq!<dt class="synopsis" id="syn$aos-$cnt">!);
              push(@result, $cgi->div({style=>"float: right;"}, $cgi->a({href=>$uri}, $cgi->img({src=>$guri})))) if $guri;
              push(@result, $cgi->a({href=>$uri}, $cgi->span($vary->{'info'} || "[$cnt.]")));
              push(@result, $cgi->span("($hits Treffer)")) if $hits;
              push(@result, '</dt>');

              push(@result, qq!<dd class="hit" id="ht$aos-$cnt" style="display: none;">!);
              push(@result, $cgi->p({class=>"ht_uri"}, $cgi->span("Target URL:"), $cgi->a({href=>$uri}, $uri)));
              push(@result, $cgi->p({class=>"ht_guri"}, $cgi->span("Preview URL:"), $cgi->a({href=>$guri}, $guri))) if $guri;
              push(@result, $cgi->p({class=>"ht_hits"}, $cgi->span("Hits:"), $vary->{hits})) if $vary->{hits};
              push(@result, $cgi->p({class=>"ht_info"}, $cgi->span("Additional Info:"), $vary->{'info'})) if $vary->{'info'};
              push(@result, '</dd>');
              
              push(@result, '<div class="floatfinish"><!-- egal --></div>');
            };
          push(@result, qq!</dl>!);
        }

      push(@result, qq!<div class="beacon" id="bc$aos" style="display: none;">!);
      foreach ( sort keys %$repos ) {
          next if /(MESSAGE|TARGET)$/;
          next unless $repos->{$_};
          $repos->{$_} =~ s!([a-z]+://\S+)!$cgi->a({href=>"$1", target=>"_blank"}, "$1")!ge;                 # URL
          $repos->{$_} =~ s!(?:\&#60;\s*)?(\w[\w.-]*)\@((?:\w[\w-]*\.)+\w+)(?:\s*\&#62;)?!&lt;$1 (at) $2&gt;!g;      # Mail Addr
          $repos->{$_} =~ s/\s*\|\s*/ | /g;                                                                  # Examples
          next if /^(FORMAT|PREFIX|REVISIT|VERSION)$/;                   # Postpone to "administrative Details"
          push(@result, $cgi->p({class=>"bc_$_"}, $cgi->span("#$_:"), $repos->{$_}));
        };
      push(@result, $cgi->p({class=>"ag_mtime"}, $cgi->span("Modified:"), $meta->{'_mtime'})) if $meta->{'_mtime'};
      push(@result, '</div>');

      push(@result, qq!<div class="aggregator" id="ag$aos" style="display: none;">!);
      foreach ( sort keys %$repos ) {
          next unless/^(FORMAT|PREFIX|REVISIT|VERSION)$/;
          next unless $repos->{$_};
          push(@result, $cgi->p({class=>"bc_$_"}, $cgi->span("#$_:"), $repos->{$_}));
        };
      push(@result, $cgi->p({class=>"ag_ftime"}, $cgi->span("Loaded:"), $meta->{'_ftime'})) if $meta->{'_ftime'};
      push(@result, $cgi->p({class=>"ag_fstat"}, $cgi->span("Load status:"), $meta->{'_fstat'})) if $meta->{'_fstat'};
      push(@result, $cgi->p({class=>"ag_utime"}, $cgi->span("Update attempt:"), $meta->{'_utime'})) if $meta->{'_utime'};
      push(@result, $cgi->p({class=>"ag_ustat"}, $cgi->span("Update status:"), $meta->{'_ustat'})) if $meta->{'_ustat'};
      push(@result, $cgi->p({class=>"ag_counti"}, $cgi->span("Identifiers:"), $meta->{'_counti'})) if $meta->{'_counti'};
      push(@result, $cgi->p({class=>"ag_countu"}, $cgi->span("Distinct Ids:"), $meta->{'_countu'})) if $meta->{'_countu'};
      push(@result, $cgi->p({class=>"ag_sort"}, $cgi->span("Sort key:"), $meta->{'_sort'})) if $meta->{'_sort'};
      push(@result, $cgi->p({class=>"ag_admin"}, $cgi->span("Remark:"), $meta->{'_admin'})) if $meta->{'_admin'};
      push(@result, '</div>');

      push(@result, '<div class="floatfinish"><!-- egal --></div>');

      push(@result, '</div>');
    };
  push(@result, '</div>');

  push(@result, '<div id="meta">');
# $cgi->span("provided by:"), 
  push(@result, $cgi->p({class=>"mt_NAME"}, $cgi->a({href=>$target}, $servicename)));
# $cgi->span("Service description:"),
  (my $descr = $beaconmeta->{'DESCRIPTION'} || $osd->{'Description'} || "") =~ s/([<>&"]|[^\x00-\x7f])/'&#'.ord($1).';'/ge;
  push(@result, $cgi->p({class=>"mt_DESCRIPTION"}, $descr));
  push(@result, '</div>');

  my($tu, $ts, $tcu, $tcs) = times();
  push(@result, sprintf("<!-- user: %.3fs + sys: %.3fs = %.3fs -->", $tu, $ts, $tu+$ts), $cgi->end_html());
  return join("\n", @result);
}

sub get_meta {
  my ($self) = @_;

  my $metasql =<<"XxX";
SELECT key, val FROM osd;
XxX
  my $metasth = $self->{dbh}->prepare($metasql) or croak("Could not prepare $metasql: ".$self->{dbh}->errstr);
  $metasth->execute() or croak("Could not execute $metasql: ".$metasth->errstr);
  my (%osd, %beaconmeta);
  while ( my @ary = $metasth->fetchrow_array ) {
      my ($key, $val) = @ary;
      next unless $val;
      if ($key =~ s/^bc// ) {        # BeaconMeta Fields
          $beaconmeta{$key} = $val}
      elsif ( exists $osd{$key} ) {
          if ( ref($osd{$key}) ) {
              push(@{$osd{$key}}, $val)}
          else {
              $osd{$key} = [$osd{$key}, $val]};
        }
      else {
          $osd{$key} = $val};
    };
  return (\%osd, \%beaconmeta);
}

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


=cut

1;
# The preceding line will help the module return a true value
