#! /usr/bin/env python
# -*- coding: utf-8 -*-


# Etalab-to-CKAN -- Tools to help migration of data.gouv.fr to CKAN
# By: Emmanuel Raviart <emmanuel@raviart.com>
#
# Copyright (C) 2013 Emmanuel Raviart
# http://gitorious.org/etalab/etalab-to-ckan
#
# This file is part of Etalab-to-CKAN.
#
# Etalab-to-CKAN is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# Etalab-to-CKAN is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


"""Retrieve JSON files french datasets stored in a Wenodata server and import them into CKAN."""


import argparse
import collections
import ConfigParser
import csv
import json
import logging
import os
#import pprint
import random
import re
import sys
import urllib
import urllib2
import urlparse

from biryani1 import baseconv, custom_conv, datetimeconv, states, strings
from ckantoolbox import ckanconv
from lxml import etree
import wenoio


app_name = os.path.splitext(os.path.basename(__file__))[0]
args = None
ckan_headers = None
conf = None
conv = custom_conv(baseconv, ckanconv, datetimeconv, states)
etalab_package_name_re = re.compile(ur'.+-(?P<etalab_id>\d{6,8})$')
existing_groups_name = None
existing_packages_name = None
existing_organizations_name = None
group_id_by_name = {}
group_name_by_organization_name = {}
grouped_packages = {}
html_parser = etree.HTMLParser()
license_id_by_title = {
    u'Licence Ouverte/Open Licence': u'fr-lo',
    }
organization_group_line_re = re.compile(ur'(?P<organization>.+)\s+\d+\s+(?P<group>.+)$')
log = logging.getLogger(app_name)
package_by_name = {}
packages_merge = []
new_organization_by_name = {}
notes_grouping_rules = {
    u"Ministère de la Culture et de la Communication": {
        u"Département des études, de la prospective et des statistiques": [
            strings.slugify(u'''Statistiques : résultats de l'enquête 2008 "Les Pratiques Culturelles des Français"'''),
            ],
        },
    }
organization_id_by_name = {}
organization_titles_by_name = {}
period_re = re.compile(ur'du (?P<day_from>[012]\d|3[01])/(?P<month_from>0\d|1[012])/(?P<year_from>[012]\d\d\d)'
    ur' au (?P<day_to>[012]\d|3[01])/(?P<month_to>0\d|1[012])/(?P<year_to>[012]\d\d\d|9999)$')
title_grouping_rules = {
    u"Agence de services et de paiement": {
        None: [
            (
                re.compile(ur"(?i)(?P<core>Registre Parcellaire Graphique : contours des îlots culturaux et leur groupe de cultures majoritaire des exploitations) - (?P<department>.+)$"),
                None,
                ('department',),
                ),
            ],
        },
    u"FranceAgriMer - Établissement national des produits de l'agriculture et de la mer": {
        None: [
            (
                re.compile(ur"(?i)(?P<core>.+?) semaine (?P<week>\d{1,2})( (?P<year>\d{4}))?$"),
                'week',
                ('week', 'year'),
                ),
            ],
        },
    u"Ministère de l’Agriculture, de l’Agroalimentaire et de la Forêt": {
        None: [
            (
                re.compile(ur"(?i)(?P<core>.+?) (?P<year>\d{4})$"),
                'year',
                ('year',),
                ),
            ],
        },
    u"Ministère de l'Economie et des Finances": {
        u"Études statistiques en matière fiscale": [
            (
                re.compile(ur"(?i)(?P<core>Impôt sur le revenu) (?P<year>\d{4}) (?P<department>.+)$"),
                'year',
                ('year', 'department'),
                ),
            (
                re.compile(ur"(?i)(?P<core>REI) (?P<year>\d{4}) (?P<department>.+)$"),
                'year',
                ('year', 'department'),
                ),
            (
                re.compile(ur"(?i)(?P<core>Taux de fiscalité directe locale et délibérations) (?P<year>\d{4}) (?P<department>.+)$"),
                'year',
                ('year', 'department'),
                ),
            ],
        },
    u"Ministère de l'Education Nationale": {
        None: [
            (
                re.compile(ur"(?i)(?P<core>.+?) - actualisation (?P<year>\d{4})$"),
                'school-year',
                ('year',),
                ),
            ],
        },
    }


def get_package_extra(package, key, default = UnboundLocalError):
    for extra in package['extras']:
        if extra['key'] == key:
            return extra['value']
    if default is UnboundLocalError:
        raise KeyError(key)
    return default


def main():
    parser = argparse.ArgumentParser(description = __doc__)
    parser.add_argument('config', help = 'path of configuration file')
    parser.add_argument('-d', '--dry-run', action = 'store_true',
        help = "simulate import, don't update CKAN repository")
    parser.add_argument('-f', '--file', action = 'store_true', help = "load packages from file")
    parser.add_argument('-o', '--offset', help = 'index of first dataset to import', type = int)
    parser.add_argument('-r', '--reset', action = 'store_true',
        help = 'erase content of CKAN database not imported by this script')
    parser.add_argument('-v', '--verbose', action = 'store_true', help = 'increase output verbosity')

    global args
    args = parser.parse_args()
    logging.basicConfig(level = logging.DEBUG if args.verbose else logging.WARNING, stream = sys.stdout)

    config_parser = ConfigParser.SafeConfigParser(dict(here = os.path.dirname(args.config)))
    config_parser.read(args.config)
    global conf
    conf = conv.check(conv.pipe(
        conv.test_isinstance(dict),
        conv.struct(
            {
                'ckan.api_key': conv.pipe(
                    conv.cleanup_line,
                    conv.not_none,
                    ),
                'ckan.site_url': conv.pipe(
                    conv.make_input_to_url(error_if_fragment = True, error_if_path = True, error_if_query = True,
                        full = True),
                    conv.not_none,
                    ),
                'user_agent': conv.pipe(
                    conv.cleanup_line,
                    conv.not_none,
                    ),
                'wenodata.site_url': conv.pipe(
                    conv.make_input_to_url(error_if_fragment = True, error_if_path = True, error_if_query = True,
                        full = True),
                    conv.not_none,
                    ),
                },
            default = 'drop',
            ),
        conv.not_none,
        ))(dict(config_parser.items('Etalab-to-CKAN')), conv.default_state)

    global ckan_headers
    ckan_headers = {
        'Authorization': conf['ckan.api_key'],
        'User-Agent': conf['user_agent'],
        }

    # Retrieve names of packages already existing in CKAN.
    request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'], '/api/3/action/package_list'),
        headers = ckan_headers)
    response = urllib2.urlopen(request)
    response_dict = json.loads(response.read())
    global existing_packages_name
    if args.reset:
        # Keep the names of all existing datasets.
        existing_packages_name = set(conv.check(conv.pipe(
            conv.ckan_json_to_package_list,
            conv.not_none,
            ))(response_dict['result'], state = conv.default_state))
    else:
        # Keep only the names of all existing Etalab datasets.
        existing_packages_name = set(
            package_name
            for package_name in conv.check(conv.pipe(
                conv.ckan_json_to_package_list,
                conv.not_none,
                ))(response_dict['result'], state = conv.default_state)
            if etalab_package_name_re.match(package_name) is not None
            )

    # Retrieve names of groups already existing in CKAN.
    request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'], '/api/3/action/group_list'),
        headers = ckan_headers)
    response = urllib2.urlopen(request)
    response_dict = json.loads(response.read())
    global existing_groups_name
    existing_groups_name = set(response_dict['result'])

    # Retrieve names of organizations already existing in CKAN.
    request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'], '/api/3/action/organization_list'),
        headers = ckan_headers)
    response = urllib2.urlopen(request)
    response_dict = json.loads(response.read())
    global existing_organizations_name
    existing_organizations_name = set(response_dict['result'])

    # Load organizations from data.gouv.fr.
    log.info('Updating organizations from data.gouv.fr')
    response = urllib2.urlopen('http://www.data.gouv.fr/Producteurs')
    html_element = etree.fromstring(response.read(), html_parser)
    for organization_element in html_element.iterfind('.//div[@class="resultatliste_item clearfix"]'):
        title = organization_element.findtext('section[@class="detail"]/h2/a')
        description = organization_element.findtext('section[@class="detail"]/p')
        image_url = urlparse.urljoin('http://www.data.gouv.fr/',
            organization_element.find('section[@class="annexe"]/img').get('src'))
        new_organization_by_name[strings.slugify(title[:100])] = dict(
            description = description,
            image_url = image_url,
            title = title,
            )

    # Load hierarchy of organizations from file.
    log.info('Updating organizations hierarchy from file')
    with open('organizations-hierarchy.txt') as organizations_file:
        for line in organizations_file:
            line = line.decode('utf-8').strip()
            assert line.count(u';;') == 2, line.encode('utf-8')
            old_title, main_title, sub_title = line.split(u';;')
            old_name = strings.slugify(old_title)[:100]
            main_title = main_title.strip() or None
            if main_title is not None:
                organization_titles_by_name[old_name] = (main_title, sub_title.strip() or None)

    # Load other organizations from file.
    log.info('Updating other organizations from file')
    with open('producteurs-orphelins.txt') as organizations_file:
        description = None
        image_url = None
        name = None
        for line in organizations_file:
            line = line.decode('utf-8')
            if u';;' in line:
                if name is not None:
                    new_organization_by_name[strings.slugify(name[:100])] = dict(
                        description = description,
                        image_url = image_url,
                        )
                organization_url, image_url, description = line.strip().split(u';;')
                assert organization_url.startswith(u'http://ckan.etalab2.fr/organization/')
                name = organization_url[len(u'http://ckan.etalab2.fr/organization/'):].strip()
                image_url = image_url.strip()
                description = description.strip()
            else:
                description += u'\n' + line.rstrip()
        if name is not None:
            new_organization_by_name[strings.slugify(name[:100])] = dict(
                description = description,
                image_url = image_url,
                )

    # Create or update default group and read their associations with organizations.
    log.info('Updating groups')
    groups_title = []
    with open('organizations-groups.txt') as organizations_groups_file:
        for line in organizations_groups_file:
            group_title = line.decode('utf-8').strip()
            if not group_title:
                break
            groups_title.append(group_title)
        for line in organizations_groups_file:
            line = line.decode('utf-8').strip()
            if not line:
                continue
            match = organization_group_line_re.match(line)
            if match is None:
                log.warning(u'Skipping invalid association: {}'.format(line))
                continue
            if match.group('group') not in groups_title:
                log.warning(u'Unexpected group in line: {}'.format(line))
                groups_title.append(match.group('group'))
            group_name_by_organization_name[strings.slugify(match.group('organization'))] = strings.slugify(match.group(
                'group'))
    for group_title in groups_title:
        log.info(u'Upserting group {0}'.format(group_title))
        upsert_group(title = group_title)

    if args.file:
        log.info(u'Reading data.gouv.fr entries from file')
        with open('fiches-data.gouv.fr.json') as entries_file:
            entry_by_etalab_id = json.load(entries_file, object_pairs_hook = collections.OrderedDict)
    else:
        log.info(u'Loading data.gouv.fr entries from Wenodata')
        entry_by_etalab_id = collections.OrderedDict()
        job = wenoio.init(server_url = conf['wenodata.site_url'])
        with job.dataset('/comarquage/metanol/fiches_data.gouv.fr').open(job) as store:
            for etalab_id, entry in store.iteritems():
                # Ignore datasets that are part of a (frequently used) web-service.
                ignore_dataset = False
                for data in entry.get(u'Données', []):
                    url = data.get('URL')
                    if url is None:
                        continue
                    url = url.split('?', 1)[0]
                    if url in (
                            u'http://www.bdm.insee.fr/bdm2/choixCriteres.action',  # 2104
                            u'http://www.bdm.insee.fr/bdm2/exporterSeries.action',  # 2104
                            u'http://www.recensement-2008.insee.fr/exportXLS.action',  # 9996
                            u'http://www.recensement-2008.insee.fr/tableauxDetailles.action',  # 9996
                            u'http://www.stats.environnement.developpement-durable.gouv.fr/Eider/selection_series_popup.do',  # 55553
                            u'http://www.recensement-2008.insee.fr/chiffresCles.action',  # 281832
                            u'http://www.recensement-2008.insee.fr/exportXLSCC.action',  # 281832
                            ):
                        ignore_dataset = True
                        break
                if ignore_dataset:
                    continue
                entry_by_etalab_id[etalab_id] = entry
        assert len(entry_by_etalab_id) > 1, entry_by_etalab_id
        log.info(u'Writing data.gouv.fr entries to file')
        with open('fiches-data.gouv.fr.json', 'w') as entries_file:
            json.dump(entry_by_etalab_id, entries_file)

    log.info('Generating datasets')
    for index, (etalab_id, entry) in enumerate(entry_by_etalab_id.iteritems()):
        if args.offset is not None and index < args.offset:
            continue

#        log.info(u'Generating dataset {0} - {1}'.format(index, entry['Titre']))

        etalab_id_str = str(etalab_id)
        package_title = u' '.join(entry['Titre'].split())  # Cleanup multiple spaces.
        package_name = u'{}-{}'.format(strings.slugify(package_title)[:100 - len(etalab_id_str) - 1],
            etalab_id_str)
        source_name = strings.slugify(entry.get('Source'))[:100]
        organization_titles = organization_titles_by_name.get(source_name)
        if organization_titles is None:
            organization_title = entry.get('Source')
            service_title = None
        else:
            organization_title, service_title = organization_titles
        organization_name = strings.slugify(organization_title)[:100]
        organization_id = organization_id_by_name.get(organization_name, UnboundLocalError)
        if organization_id is UnboundLocalError:
            organization_id = upsert_organization(title = organization_title)
        license_id = conv.check(conv.pipe(
            conv.test_in(license_id_by_title),
            conv.translate(license_id_by_title)
            ))(entry.get('Licence', {}).get('Titre'), state = conv.default_state)
        extras = [
            dict(
                # deleted = True,
                key = key,
                value = value,
                )
            for key, value in entry.iteritems()
            if key not in (
                u'Couverture géographique',
                u'Date de dernière modification',
                u'Date de publication',
                u'Description',
                u'Documents annexes',
                u'Données',
                u'Licence',
                u'Mots-clés',
                u'Période',
                u'Source',
                u'Titre',
                )
            if isinstance(value, basestring)
            ]

        resources = []
        for data in entry.get(u'Données', []) + entry.get(u'Documents annexes', []):
            resource_name = u' '.join(data['Titre'].split()) if data.get('Titre') else None  # Cleanup spaces.
            resource_name = {
                u'Accéder au service de téléchargement': None,
                u'Télécharger': None,
                }.get(resource_name, resource_name)
            resources.append(dict(
                created = entry.get(u'Date de publication'),
                format = data.get('Format'),
                last_modified = entry.get(u'Date de dernière modification'),
                name = resource_name,
                # package_id (string) – id of package that the resource needs should be added to.
                url = data['URL'],
#                revision_id – (optional)
#                description (string) – (optional)
#                hash (string) – (optional)
#                resource_type (string) – (optional)
#                mimetype (string) – (optional)
#                mimetype_inner (string) – (optional)
#                webstore_url (string) – (optional)
#                cache_url (string) – (optional)
#                size (int) – (optional)
#                cache_last_updated (iso date string) – (optional)
#                webstore_last_updated (iso date string) – (optional)
                ))

        package = dict(
            author = organization_title,  # TODO
#                author_email = ,
            extras = extras,
            # groups is added below.
            license_id = license_id,
            maintainer = service_title or u'',  # Don't duplicate with the author, because it is useless.
#                maintainer_email = ,
            name = package_name,
            notes = entry.get('Description'),
            owner_org = organization_id,
#                relationships_as_object (list of relationship dictionaries) – see package_relationship_create() for the format of relationship dictionaries (optional)
#                relationships_as_subject (list of relationship dictionaries) – see package_relationship_create() for the format of relationship dictionaries (optional)
            resources = resources,
            # state = 'active',
            tags = [
                dict(
                    name = tag_name,
#                        vocabulary_id (string) – the name or id of the vocabulary that the new tag should be added to, e.g. 'Genre'
                    )
                for tag_name in (
                    strings.slugify(keyword)[:100]
                    for keyword in entry.get(u'Mots-clés', [])
                    if keyword is not None
                    )
                if len(tag_name) >= 2
                ],
            title = package_title,
#                type (string) – the type of the dataset (optional), IDatasetForm plugins associate themselves with different dataset types and provide custom dataset handling behaviour for these types
#                url (string) – a URL for the dataset’s source (optional)
#                version (string, no longer than 100 characters) – (optional)
            )

        group_name = group_name_by_organization_name.get(organization_name)
        if group_name is not None:
            group_id = group_id_by_name.get(group_name)
            if group_id is not None:
                package['groups'] = [
                    dict(id = group_id),
                    ]

        period = entry.get(u'Période')
        if period is not None:
            match = period_re.match(period)
            if match is not None:
                set_package_extra(package, u'temporal_coverage_from',
                    u'{}-{}-{}'.format(match.group('year_from'), match.group('month_from'), match.group('day_from')))
                year_to = match.group('year_to')
                if year_to == u'9999':
                    year_to = '2013'
                set_package_extra(package, u'temporal_coverage_to',
                    u'{}-{}-{}'.format(year_to, match.group('month_to'), match.group('day_to')))

        territorial_coverage = entry.get(u'Territoires couverts')
        if territorial_coverage:
            set_package_extra(package, u'territorial_coverage', u','.join(
                u'{}/{}'.format(territory['kind'], territory['code'])
                for territory in territorial_coverage
                ))

        assert package_name not in package_by_name, package_name
        package_by_name[package_name] = package

        # Group packages having the same title except a date and/or other fields (like territory).
        packages_infos_by_pattern = grouped_packages.setdefault(organization_title, {}).setdefault(
            service_title, {})
        service_title_grouping_rules = title_grouping_rules.get(organization_title, {}).get(service_title)
        if service_title_grouping_rules is not None:
            for rule_index, (package_title_re, repetition_type, fields) in enumerate(service_title_grouping_rules):
                match = package_title_re.match(package_title)
                if match is None:
                    packages_infos_by_pattern.setdefault(None, []).append((package_name, package_title))
                else:
                    packages_infos_by_pattern.setdefault((rule_index, strings.slugify(match.group('core')),
                        repetition_type, fields), []).append((package_name, match.groupdict()))

        # Group packages having the same description.
        package_notes_slug = strings.slugify(package.get('notes')) or None
        if package_notes_slug is not None:
            service_notes_grouping_rules = notes_grouping_rules.get(organization_title, {}).get(service_title)
            if service_notes_grouping_rules is not None:
                for rule_index, notes_slug in enumerate(service_notes_grouping_rules, 100):
                    if package_notes_slug == notes_slug:
                        packages_infos_by_pattern.setdefault((rule_index, None, None, None), []).append((
                            package_name, dict(core = package['notes'])))
                        break

#            elif organization_title == u"Ministère de l'Economie et des Finances":
#                if service_title == u"Études statistiques en matière fiscale":
#                    match = re.match(ur'(?i)Impôt sur le revenu (?P<year>\d{4}) (?P<department>.+)$', package_title)
#                    if match is not None:
#                        package['title'] = package_title = u"Impôt sur le revenu"
#                        package['name'] = package_name = u'{}-{}'.format(
#                            strings.slugify(package_title)[:100 - len(etalab_id_str) - 1], etalab_id_str)
#                        set_package_extra(package, u'temporal_coverage_from', u'{}-01-01'.format(match.group('year')))
#                        set_package_extra(package, u'temporal_coverage_to', u'{}-12-31'.format(match.group('year')))
#                        set_package_extra(package, u'territorial_coverage', u'Country/FR')
#                        set_package_extra(package, u'territorial_coverage_granularity', u'commune')

#                        assert len(package['resources']) == 1, package
#                        resource = package['resources'][0]
#                        original_resource_name = resource['name']
#                        resource['description'] = package.pop('notes', None)
#                        resource['name'] = u"Impôt sur le revenu {} {}".format(match.group('year'),
#                            match.group('department').upper())

#                        if package_name in package_by_name:
#                            package2 = package
#                            package = package_by_name[package_name]
#                            set_package_extra(package, u'temporal_coverage_from', min(
#                                get_package_extra(package, u'temporal_coverage_from'),
#                                get_package_extra(package2, u'temporal_coverage_from')))
#                            set_package_extra(package, u'temporal_coverage_to', max(
#                                get_package_extra(package, u'temporal_coverage_to'),
#                                get_package_extra(package2, u'temporal_coverage_to')))
#                            package['resources'].append(resource)
#                            package['resources'].sort(key = lambda resource: resource['name'])
#                        else:
#                            package_by_name[package_name] = package

#                        packages_merge.append((
#                            organization_title,
#                            service_title,
#                            original_package_title,
#                            original_resource_name,
#                            package_title,
#                            resource['name'],
#                            ))
#                        continue

#                    match = re.match(ur'(?i)REI (?P<year>\d{4}) (?P<department>.+)$', package_title)
#                    if match is not None:
#                        package['title'] = package_title = u"Recensement des éléments d'imposition à la fiscalité directe locale (REI)"
#                        package['name'] = package_name = u'{}-{}'.format(
#                            strings.slugify(package_title)[:100 - len(etalab_id_str) - 1], etalab_id_str)
#                        package['notes'] = u'''\
#- Taxe d'habitation
#- Taxe foncière sur les propriétés bâties
#- Taxe foncière sur les propriétés non bâties
#- Taxe professionnelle
#- Taxe pour Chambre de commerce et d'industrie
#- Taxe pour Chambre des métiers
#- Taxe pour Chambre d'agriculture ou CAAA
#- Taxe d'enlèvement des ordures ménagères
#'''
#                        set_package_extra(package, u'temporal_coverage_from', u'{}-01-01'.format(match.group('year')))
#                        set_package_extra(package, u'temporal_coverage_to', u'{}-12-31'.format(match.group('year')))
#                        set_package_extra(package, u'territorial_coverage', u'Country/FR')
#                        set_package_extra(package, u'territorial_coverage_granularity', u'commune')

#                        assert len(package['resources']) == 1, package
#                        resource = package['resources'][0]
#                        original_resource_name = resource['name']
#                        # resource['description'] = package.pop('notes', None)
#                        resource['name'] = u"REI {} {}".format(match.group('year'), match.group('department').upper())

#                        if package_name in package_by_name:
#                            package2 = package
#                            package = package_by_name[package_name]
#                            set_package_extra(package, u'temporal_coverage_from', min(
#                                get_package_extra(package, u'temporal_coverage_from'),
#                                get_package_extra(package2, u'temporal_coverage_from')))
#                            set_package_extra(package, u'temporal_coverage_to', max(
#                                get_package_extra(package, u'temporal_coverage_to'),
#                                get_package_extra(package2, u'temporal_coverage_to')))
#                            package['resources'].append(resource)
#                            package['resources'].sort(key = lambda resource: resource['name'])
#                        else:
#                            package_by_name[package_name] = package

#                        packages_merge.append((
#                            organization_title,
#                            service_title,
#                            original_package_title,
#                            original_resource_name,
#                            package_title,
#                            resource['name'],
#                            ))
#                        continue

#                    match = re.match(
#                        ur'(?i)Taux de fiscalité directe locale et délibérations (?P<year>\d{4}) (?P<department>.+)$',
#                        package_title)
#                    if match is not None:
#                        package['title'] = package_title = u"Taux de fiscalité directe locale et délibérations"
#                        package['name'] = package_name = u'{}-{}'.format(
#                            strings.slugify(package_title)[:100 - len(etalab_id_str) - 1], etalab_id_str)
#                        set_package_extra(package, u'temporal_coverage_from', u'{}-01-01'.format(match.group('year')))
#                        set_package_extra(package, u'temporal_coverage_to', u'{}-12-31'.format(match.group('year')))
#                        set_package_extra(package, u'territorial_coverage', u'Country/FR')
#                        set_package_extra(package, u'territorial_coverage_granularity', u'commune')

#                        assert len(package['resources']) == 1, package
#                        resource = package['resources'][0]
#                        original_resource_name = resource['name']
#                        # resource['description'] = package.pop('notes', None)
#                        resource['name'] = u"Taux de fiscalité directe locale et délibérations {} {}".format(
#                            match.group('year'), match.group('department').upper())

#                        if package_name in package_by_name:
#                            package2 = package
#                            package = package_by_name[package_name]
#                            set_package_extra(package, u'temporal_coverage_from', min(
#                                get_package_extra(package, u'temporal_coverage_from'),
#                                get_package_extra(package2, u'temporal_coverage_from')))
#                            set_package_extra(package, u'temporal_coverage_to', max(
#                                get_package_extra(package, u'temporal_coverage_to'),
#                                get_package_extra(package2, u'temporal_coverage_to')))
#                            package['resources'].append(resource)
#                            package['resources'].sort(key = lambda resource: resource['name'])
#                        else:
#                            package_by_name[package_name] = package

#                        packages_merge.append((
#                            organization_title,
#                            service_title,
#                            original_package_title,
#                            original_resource_name,
#                            package_title,
#                            resource['name'],
#                            ))
#                        continue

    log.info(u'Merging datasets')
    for organization_title, organization_grouped_packages in grouped_packages.iteritems():
        for service_title, packages_infos_by_pattern in organization_grouped_packages.iteritems():
            # First, try to regroup ungrouped packages with a group that uses the name of the package as core.
            ungrouped_packages_infos = packages_infos_by_pattern.pop(None, [])
            for package_name, package_title in ungrouped_packages_infos:
                package_slug = strings.slugify(package_title)
                for (rule_index, core, repetition_type, fields), packages_infos in packages_infos_by_pattern.iteritems():
                    if package_slug == core:
                        packages_infos.insert(0, (package_name, dict(core = package_title)))
                        break
            # Merge packages with the same core.
            for (rule_index, core, repetition_type, fields), packages_infos in packages_infos_by_pattern.iteritems():
                if len(packages_infos) == 1:
                    continue
                merged_package = None
                packages_infos.sort(key = lambda (package_name, vars): strings.slugify(package_name))
                for package_index, (package_name, vars) in enumerate(packages_infos):
                    package = package_by_name.pop(package_name)

                    if repetition_type == 'school-year':
                        assert get_package_extra(package, u'temporal_coverage_from', None) is not None, package
                        assert get_package_extra(package, u'temporal_coverage_to', None) is not None, package
                    elif repetition_type == 'week':
                        assert get_package_extra(package, u'temporal_coverage_from', None) is not None, package
                        assert get_package_extra(package, u'temporal_coverage_to', None) is not None, package
                    elif repetition_type == 'year':
                        if vars.get('year'):
                            set_package_extra(package, u'temporal_coverage_from', u'{}-01-01'.format(vars['year']))
                            set_package_extra(package, u'temporal_coverage_to', u'{}-12-31'.format(vars['year']))
                        else:
                            assert get_package_extra(package, u'temporal_coverage_from', None) is not None, package
                            assert get_package_extra(package, u'temporal_coverage_to', None) is not None, package
                    else:
                        assert repetition_type is None, repetition_type

                    if package['resources']:
                        original_first_resource_name = package['resources'][0]['name']
                        for resource_index, resource in enumerate(package['resources']):
                            if resource_index == 0 and resource.get('description') is None:
                                resource['description'] = package.get('notes')
                            if resource.get('name') is None:
                                resource['name'] = package['title']
                                if resource_index > 0:
                                    resource['name'] += u'- document {}'.format(resource_index + 1)
                            else:
                                for field in (fields or []):
                                    field_value = vars.get(field)
                                    if field_value and field_value not in resource['name']:
                                        resource['name'] += u' - {}'.format(field_value)

                        if package_index == 0:
                            merged_package = package.copy()
                            merged_package['title'] = vars['core']
                            merged_package['name'] = merged_package_name = u'{}-00000000'.format(
                                strings.slugify(merged_package['title'])[:100 - len(u'00000000') - 1])
                            package_by_name[merged_package_name] = merged_package
                        else:
                            if repetition_type is not None:
                                set_package_extra(merged_package, u'temporal_coverage_from', min(
                                    get_package_extra(merged_package, u'temporal_coverage_from'),
                                    get_package_extra(package, u'temporal_coverage_from')))
                                set_package_extra(merged_package, u'temporal_coverage_to', max(
                                    get_package_extra(merged_package, u'temporal_coverage_to'),
                                    get_package_extra(package, u'temporal_coverage_to')))
                            merged_package['resources'].extend(package['resources'])
                        merged_first_resource_name = package['resources'][0]['name']
                    else:
                        original_first_resource_name = None
                        merged_first_resource_name = None

                    packages_merge.append((
                        organization_title,
                        service_title,
                        package['title'],
                        original_first_resource_name,
                        merged_package['title'],
                        merged_first_resource_name,
                        ))

    for package_name, package in package_by_name.iteritems():
        if package_name in existing_packages_name:
            existing_packages_name.remove(package_name)
            if not args.dry_run:
                request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'],
                    '/api/3/action/package_update?id={}'.format(package_name)), headers = ckan_headers)
                try:
                    response = urllib2.urlopen(request, urllib.quote(json.dumps(package)))
                except urllib2.HTTPError as response:
                    response_text = response.read()
                    try:
                        response_dict = json.loads(response_text)
                    except ValueError:
                        log.error(u'An exception occured while updating package: {}'.format(package))
                        log.error(response_text)
                        continue
                    log.error(u'An error occured while updating package: {}'.format(package))
                    for key, value in response_dict.iteritems():
                        print '{} = {}'.format(key, value)
                else:
                    assert response.code == 200
                    response_dict = json.loads(response.read())
                    assert response_dict['success'] is True
#                    updated_package = response_dict['result']
#                    pprint.pprint(updated_package)
        elif not args.dry_run:
            request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'], '/api/3/action/package_create'),
                headers = ckan_headers)
            try:
                response = urllib2.urlopen(request, urllib.quote(json.dumps(package)))
            except urllib2.HTTPError as response:
                response_text = response.read()
                try:
                    response_dict = json.loads(response_text)
                except ValueError:
                    log.error(u'An exception occured while creating package: {}'.format(package))
                    log.error(response_text)
                    continue
                error = response_dict.get('error', {})
                if error.get('__type') == u'Validation Error' and error.get('name'):
                    # A package with the same name already exists. Maybe it is deleted. Undelete it.
                    package['state'] = 'active'
                    request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'],
                        '/api/3/action/package_update?id={}'.format(package_name)), headers = ckan_headers)
                    try:
                        response = urllib2.urlopen(request, urllib.quote(json.dumps(package)))
                    except urllib2.HTTPError as response:
                        response_text = response.read()
                        try:
                            response_dict = json.loads(response_text)
                        except ValueError:
                            log.error(u'An exception occured while undeleting package: {}'.format(package))
                            log.error(response_text)
                            continue
                        log.error(u'An error occured while undeleting package: {}'.format(package))
                        for key, value in response_dict.iteritems():
                            print '{} = {}'.format(key, value)
                    else:
                        assert response.code == 200
                        response_dict = json.loads(response.read())
                        assert response_dict['success'] is True
#                        updated_package = response_dict['result']
#                        pprint.pprint(updated_package)
                else:
                    log.error(u'An error occured while creating package: {}'.format(package))
                    for key, value in response_dict.iteritems():
                        print '{} = {}'.format(key, value)
            else:
                assert response.code == 200
                response_dict = json.loads(response.read())
                assert response_dict['success'] is True
#                created_package = response_dict['result']
#                pprint.pprint(created_package)

    print 'Obsolete packages: {}'.format(existing_packages_name)
    if not args.dry_run:
        for package_name in existing_packages_name:
            # Retrieve package id (needed for delete).
            request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'],
                '/api/3/action/package_show?id={}'.format(package_name)), headers = ckan_headers)
            response = urllib2.urlopen(request)
            response_dict = json.loads(response.read())
            existing_package = response_dict['result']

            # TODO: To replace with package_purge when it is available.
            request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'],
                '/api/3/action/package_delete?id={}'.format(package_name)), headers = ckan_headers)
            response = urllib2.urlopen(request, urllib.quote(json.dumps(existing_package)))
            response_dict = json.loads(response.read())
#            deleted_package = response_dict['result']
#            pprint.pprint(deleted_package)

    if args.reset:
        print 'Obsolete groups: {}'.format(existing_groups_name)
        if not args.dry_run:
            for group_name in existing_groups_name:
                # Retrieve group id (needed for delete).
                request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'],
                    '/api/3/action/group_show?id={}'.format(group_name)), headers = ckan_headers)
                response = urllib2.urlopen(request)
                response_dict = json.loads(response.read())
                existing_group = response_dict['result']

                # TODO: To replace with group_purge when it is available.
                request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'],
                    '/api/3/action/group_delete?id={}'.format(group_name)), headers = ckan_headers)
                response = urllib2.urlopen(request, urllib.quote(json.dumps(existing_group)))
                response_dict = json.loads(response.read())
#                deleted_group = response_dict['result']
#                pprint.pprint(deleted_group)

    if args.reset:
        print 'Obsolete organizations: {}'.format(existing_organizations_name)
        if not args.dry_run:
            for organization_name in existing_organizations_name:
                # Retrieve organization id (needed for delete).
                request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'],
                    '/api/3/action/organization_show?id={}'.format(organization_name)), headers = ckan_headers)
                response = urllib2.urlopen(request)
                response_dict = json.loads(response.read())
                existing_organization = response_dict['result']

                # TODO: To replace with organization_purge when it is available.
                request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'],
                    '/api/3/action/organization_delete?id={}'.format(organization_name)), headers = ckan_headers)
                response = urllib2.urlopen(request, urllib.quote(json.dumps(existing_organization)))
                response_dict = json.loads(response.read())
#                deleted_organization = response_dict['result']
#                pprint.pprint(deleted_organization)

    if packages_merge:
        with open('jeux-de-donnees-fusionnes.txt', 'w') as packages_merge_file:
            packages_merge_csv_writer = csv.writer(packages_merge_file, delimiter = ';', quotechar = '"',
                quoting = csv.QUOTE_MINIMAL)
            packages_merge_csv_writer.writerow([
                'Organisation',
                'Service',
                'Jeu de données initial',
                'Resource initiale',
                'Jeu de données fusionné',
                'Resource après fusion',
                ])
            for package_merge in sorted(packages_merge):
                packages_merge_csv_writer.writerow([
                    cell.encode('utf-8') if cell is not None else ''
                    for cell in package_merge
                    ])

    return 0


def set_package_extra(package, key, value):
    for extra in package['extras']:
        if extra['key'] == key:
            extra['value'] = value
            return
    package['extras'].append(dict(
        key = key,
        value = value,
        ))


def upsert_group(description = None, image_url = None, title = None):
    name = strings.slugify(title[:100])
    group = dict(
        name = name,
        title = title,
#        description (string) – the description of the group (optional)
#        image_url (string) – the URL to an image to be displayed on the group’s page (optional)
#        type (string) – the type of the group (optional), IGroupForm plugins associate themselves with different group types and provide custom group handling behaviour for these types Cannot be ‘organization’
#        state (string) – the current state of the group, e.g. 'active' or 'deleted', only active groups show up in search results and other lists of groups, this parameter will be ignored if you are not authorized to change the state of the group (optional, default: 'active')
#        approval_status (string) – (optional)
#        extras (list of dataset extra dictionaries) – the group’s extras (optional), extras are arbitrary (key: value) metadata items that can be added to groups, each extra dictionary should have keys 'key' (a string), 'value' (a string), and optionally 'deleted'
#        packages (list of dictionaries) – the datasets (packages) that belong to the group, a list of dictionaries each with keys 'name' (string, the id or name of the dataset) and optionally 'title' (string, the title of the dataset)
#        groups (list of dictionaries) – the groups that belong to the group, a list of dictionaries each with key 'name' (string, the id or name of the group) and optionally 'capacity' (string, the capacity in which the group is a member of the group)
#        users (list of dictionaries) – the users that belong to the group, a list of dictionaries each with key 'name' (string, the id or name of the user) and optionally 'capacity' (string, the capacity in which the user is a member of the group)
        )
    if name in existing_groups_name:
        existing_groups_name.remove(name)

        # Retrieve group id (needed for update).
        request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'],
            '/api/3/action/group_show?id={}'.format(name)), headers = ckan_headers)
        response = urllib2.urlopen(request)
        response_dict = json.loads(response.read())
        existing_group = response_dict['result']

        group['id'] = existing_group['id']
# Currently (CKAN 2.0), updating a group remove all its datasets, so we never update an existing group.
#        request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'],
#            '/api/3/action/group_update?id={}'.format(name)), headers = ckan_headers)
#        try:
#            response = urllib2.urlopen(request, urllib.quote(json.dumps(group)))
#        except urllib2.HTTPError as response:
#            response_text = response.read()
#            try:
#                response_dict = json.loads(response_text)
#            except ValueError:
#                log.error(u'An exception occured while updating group: {0}'.format(group))
#                log.error(response_text)
#                group_id_by_name[name] = None
#                return None
#            print '\n\nupdate'
#            for key, value in response_dict.iteritems():
#                print '{} = {}'.format(key, value)
#            return None
#        else:
#            assert response.code == 200
#            response_dict = json.loads(response.read())
#            assert response_dict['success'] is True
##            updated_group = response_dict['result']
##            pprint.pprint(updated_group)
    elif args.dry_run:
        # Generate a random group iD.
        group['id'] = u'{}-{}'.format(group['name'], random.randrange(1000000))
    else:
        request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'], '/api/3/action/group_create'),
            headers = ckan_headers)
        try:
            response = urllib2.urlopen(request, urllib.quote(json.dumps(group)))
        except urllib2.HTTPError as response:
            response_text = response.read()
            try:
                response_dict = json.loads(response_text)
            except ValueError:
                log.error(u'An exception occured while creating group: {0}'.format(group))
                log.error(response_text)
                group_id_by_name[name] = None
                return None
            print '\n\ncreate'
            for key, value in response_dict.iteritems():
                print '{} = {}'.format(key, value)
            return None
        else:
            assert response.code == 200
            response_dict = json.loads(response.read())
            assert response_dict['success'] is True
            created_group = response_dict['result']
#            pprint.pprint(created_group)
            group['id'] = created_group['id']
    assert group['name'] == name
    group_id_by_name[name] = group['id']
    return group['id']


def upsert_organization(description = None, image_url = None, title = None):
    name = strings.slugify(title[:100])
    organization = new_organization_by_name.get(name)
    if organization is None:
        organization = dict(
            description = description,
            image_url = image_url,
            name = name,
            title = title,
            )
    else:
        organization['name'] = name
        if organization.get('title') is None:
            organization['title'] = title
    if name in existing_organizations_name:
        existing_organizations_name.remove(name)

        # Retrieve organization id (needed for update).
        request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'],
            '/api/3/action/organization_show?id={}'.format(name)), headers = ckan_headers)
        response = urllib2.urlopen(request)
        response_dict = json.loads(response.read())
        existing_organization = response_dict['result']

        organization['id'] = existing_organization['id']
        if not args.dry_run:
            request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'],
                '/api/3/action/organization_update?id={}'.format(name)), headers = ckan_headers)
            try:
                response = urllib2.urlopen(request, urllib.quote(json.dumps(organization)))
            except urllib2.HTTPError as response:
                response_text = response.read()
                try:
                    response_dict = json.loads(response_text)
                except ValueError:
                    log.error(u'An exception occured while updating organization: {0}'.format(organization))
                    log.error(response_text)
                    organization_id_by_name[name] = None
                    return None
                print '\n\nupdate'
                for key, value in response_dict.iteritems():
                    print '{} = {}'.format(key, value)
                return None
            else:
                assert response.code == 200
                response_dict = json.loads(response.read())
                assert response_dict['success'] is True
#                updated_organization = response_dict['result']
#                pprint.pprint(updated_organization)
    elif args.dry_run:
        # Generate a random organization iD.
        organization['id'] = u'{}-{}'.format(organization['name'], random.randrange(1000000))
    else:
        request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'], '/api/3/action/organization_create'),
            headers = ckan_headers)
        try:
            response = urllib2.urlopen(request, urllib.quote(json.dumps(organization)))
        except urllib2.HTTPError as response:
            response_text = response.read()
            try:
                response_dict = json.loads(response_text)
            except ValueError:
                log.error(u'An exception occured while creating organization: {0}'.format(organization))
                log.error(response_text)
                organization_id_by_name[name] = None
                return None
            print '\n\ncreate'
            for key, value in response_dict.iteritems():
                print '{} = {}'.format(key, value)
            return None
        else:
            assert response.code == 200
            response_dict = json.loads(response.read())
            assert response_dict['success'] is True
            created_organization = response_dict['result']
#            pprint.pprint(created_organization)
            organization['id'] = created_organization['id']
    assert organization['name'] == name
    organization_id_by_name[name] = organization['id']
    return organization['id']


if __name__ == '__main__':
    sys.exit(main())
