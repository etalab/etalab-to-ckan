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
import json
import logging
import os
import pprint
import re
import sys
import urllib
import urllib2
import urlparse

from biryani1 import strings
from lxml import etree
import wenoio


app_name = os.path.splitext(os.path.basename(__file__))[0]
args = None
existing_organizations_name = None
group_id_by_name = {}
group_name_by_organization_name = {}
html_parser = etree.HTMLParser()
organization_group_line_re = re.compile(ur'(?P<organization>.+)\s+\d+\s+(?P<group>.+)$')
log = logging.getLogger(app_name)
new_organization_by_name = {}
organization_id_by_name = {}
organization_titles_by_name = {}


def main():
    parser = argparse.ArgumentParser(description = __doc__)
    parser.add_argument('-o', '--offset', help = 'index of first dataset to import', type = int)
    parser.add_argument('-u', '--user-api-key', help = 'CKAN user API key', required = True)
    parser.add_argument('-v', '--verbose', action = 'store_true', help = 'increase output verbosity')

    server_group = parser.add_argument_group('Wenodata server')
    server_group.add_argument('-w', '--wenodata', help = 'URL of Wenodata server')

    local_group = parser.add_argument_group('Local Wenodata tree')
    local_group.add_argument('-l', '--local', help = 'local directory containing Wenodata datasets')

    authentication_group = parser.add_argument_group('Wenodata servers authentication')
    authentication_group.add_argument('-a', '--auth', default = 'https://wenou.wenoit.org',
        help = 'authentication server URL (Wenou)')
    authentication_group.add_argument('-e', '--email', help = 'authentication email address')
    authentication_group.add_argument('-p', '--password', help = 'authentication password')

    global args
    args = parser.parse_args()
    logging.basicConfig(level = logging.DEBUG if args.verbose else logging.WARNING, stream = sys.stdout)

    job = wenoio.init(authentication_server_url = args.auth, email = args.email, local_nodes_path = args.local,
        password = args.password, server_url = args.wenodata)

    # Retrieve group id (needed for update).
    request = urllib2.Request('http://ckan.quoi-ou.fr/api/3/action/organization_list')
    request.add_header('Authorization', args.user_api_key)
    response = urllib2.urlopen(request)
    response_dict = json.loads(response.read())
    global existing_organizations_name
    existing_organizations_name = set(response_dict['result'])

    # Load organizations from data.gouv.fr and upload them to CKAN.
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
#    log.info('Organizations: {}'.format(sorted(organization_id_by_name.iterkeys())))

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
                assert organization_url.startswith(u'http://ckan.easter-eggs.com/organization/')
                name = organization_url[len(u'http://ckan.easter-eggs.com/organization/'):].strip()
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

    log.info('Updating datasets')
    with job.dataset('/comarquage/metanol/fiches_data.gouv.fr').open(job) as store:
        for index, (wenodata_id, entry) in enumerate(store.iteritems()):
            if args.offset is not None and index < args.offset:
                continue

            # Ignore datasets that are part of a (frequently used) web-service.
            ignore_dataset = False
            for data in entry.get(u'Données', []):
                url = data.get('URL')
                if url is None:
                    continue
                url = url.split('?', 1)[0]
                if url in (
                        u'http://www.bdm.insee.fr/bdm2/choixCriteres.action',  # 2104
                        u'http://www.bdm.insee.fr/bdm2/exporterSeries.action',  #  2104
                        u'http://www.recensement-2008.insee.fr/exportXLS.action',  #  9996
                        u'http://www.recensement-2008.insee.fr/tableauxDetailles.action',  #  9996
                        u'http://www.stats.environnement.developpement-durable.gouv.fr/Eider/selection_series_popup.do',  #  55553
                        u'http://www.recensement-2008.insee.fr/chiffresCles.action',  #  281832
                        u'http://www.recensement-2008.insee.fr/exportXLSCC.action',  #  281832
                        ):
                    ignore_dataset = True
                    break
            if ignore_dataset:
                continue

            log.info(u'Upserting dataset {0} - {1}'.format(index, entry['Titre']))

            wenodata_id_str = str(wenodata_id)
            package_name = u'{}-{}'.format(strings.slugify(entry['Titre'])[:100 - len(wenodata_id_str) - 1],
                wenodata_id_str)
            source_name = strings.slugify(entry.get('Source'))[:100]
            organization_titles = organization_titles_by_name.get(source_name)
            if organization_titles is None:
                organization_title = entry.get('Source')
                organization_sub_title = None
            else:
                organization_title, organization_sub_title = organization_titles
            organization_name = strings.slugify(organization_title)[:100]
            organization_id = organization_id_by_name.get(organization_name, UnboundLocalError)
            if organization_id is UnboundLocalError:
                organization_id = upsert_organization(title = organization_title)
            package = dict(
                author = organization_title,  # TODO
#                author_email = ,
#                extras (list of dataset extra dictionaries) – the dataset’s extras (optional), extras are arbitrary (key: value) metadata items that can be added to datasets, each extra dictionary should have keys 'key' (a string), 'value' (a string), and optionally 'deleted'
                # groups is added below.
#                license_id (license id string) – the id of the dataset’s license, see license_list() for available values (optional)
                maintainer = organization_sub_title or u'',  # Don't duplicate with the author, because it is useless.
#                maintainer_email = ,
                name = package_name,
                notes = entry.get('Description'),
                owner_org = organization_id,
#                relationships_as_object (list of relationship dictionaries) – see package_relationship_create() for the format of relationship dictionaries (optional)
#                relationships_as_subject (list of relationship dictionaries) – see package_relationship_create() for the format of relationship dictionaries (optional)
                resources = [
                    dict(
                        # package_id (string) – id of package that the resource needs should be added to.
                        format = data.get('Format'),
                        name = data.get('Titre'),
                        url = data['URL'],
#                        revision_id – (optional)
#                        description (string) – (optional)
#                        format (string) – (optional)
#                        hash (string) – (optional)
#                        resource_type (string) – (optional)
#                        mimetype (string) – (optional)
#                        mimetype_inner (string) – (optional)
#                        webstore_url (string) – (optional)
#                        cache_url (string) – (optional)
#                        size (int) – (optional)
#                        created (iso date string) – (optional)
#                        last_modified (iso date string) – (optional)
#                        cache_last_updated (iso date string) – (optional)
#                        webstore_last_updated (iso date string) – (optional)
                        )
                    for data in entry.get(u'Données', [])
                    ],
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
                title = entry['Titre'],
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
            request = urllib2.Request('http://ckan.quoi-ou.fr/api/3/action/package_create')
            request.add_header('Authorization', args.user_api_key)
            try:
                response = urllib2.urlopen(request, urllib.quote(json.dumps(package)))
            except urllib2.HTTPError as response:
                response_text = response.read()
                try:
                    response_dict = json.loads(response_text)
                except ValueError:
                    log.error(u'{0} - An exception occured while creating package: {1}'.format(index, package))
                    log.error(response_text)
                    continue
                if response.code == 409 and response_dict.get('error', {}).get('name'):
                    # Package already exists. Update it.
                    request = urllib2.Request('http://ckan.quoi-ou.fr/api/3/action/package_update?id={}'.format(
                        package_name))
                    request.add_header('Authorization', args.user_api_key)
                    try:
                        response = urllib2.urlopen(request, urllib.quote(json.dumps(package)))
                    except urllib2.HTTPError as response:
                        response_text = response.read()
                        try:
                            response_dict = json.loads(response_text)
                        except ValueError:
                            log.error(u'{0} - An exception occured while updating package: {1}'.format(index, package))
                            log.error(response_text)
                            continue
                        for key, value in response_dict.iteritems():
                            print '{} = {}'.format(key, value)
                    else:
                        assert response.code == 200
                        response_dict = json.loads(response.read())
                        assert response_dict['success'] is True
#                        updated_package = response_dict['result']
#                        pprint.pprint(updated_package)
                else:
                    for key, value in response_dict.iteritems():
                        print '{} = {}'.format(key, value)
            else:
                assert response.code == 200
                response_dict = json.loads(response.read())
                assert response_dict['success'] is True
#                created_package = response_dict['result']
#                pprint.pprint(created_package)

    print existing_organizations_name
    for organization_name in existing_organizations_name:
        # Retrieve organization id (needed for delete).
        request = urllib2.Request('http://ckan.quoi-ou.fr/api/3/action/organization_show?id={}'.format(
            organization_name))
        request.add_header('Authorization', args.user_api_key)
        response = urllib2.urlopen(request)
        response_dict = json.loads(response.read())
        existing_organization = response_dict['result']

        # TODO: To replace with organization_purge when it is available.
        request = urllib2.Request('http://ckan.quoi-ou.fr/api/3/action/organization_delete?id={}'.format(
            organization_name))
        request.add_header('Authorization', args.user_api_key)
        response = urllib2.urlopen(request, urllib.quote(json.dumps(existing_organization)))
        response_dict = json.loads(response.read())
#        deleted_organization = response_dict['result']
#        pprint.pprint(deleted_organization)

    return 0


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
    request = urllib2.Request('http://ckan.quoi-ou.fr/api/3/action/group_create')
    request.add_header('Authorization', args.user_api_key)
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
        if response.code == 409 and response_dict.get('error', {}).get('name'):
            # Package already exists. Update it.

            # Retrieve group id (needed for update).
            request = urllib2.Request('http://ckan.quoi-ou.fr/api/3/action/group_show?id={}'.format(name))
            request.add_header('Authorization', args.user_api_key)
            response = urllib2.urlopen(request)
            response_dict = json.loads(response.read())
            existing_group = response_dict['result']

            group['id'] = existing_group['id']
            request = urllib2.Request('http://ckan.quoi-ou.fr/api/3/action/group_update?id={}'.format(name))
            request.add_header('Authorization', args.user_api_key)
            try:
                response = urllib2.urlopen(request, urllib.quote(json.dumps(group)))
            except urllib2.HTTPError as response:
                response_text = response.read()
                try:
                    response_dict = json.loads(response_text)
                except ValueError:
                    log.error(u'An exception occured while updating group: {0}'.format(group))
                    log.error(response_text)
                    group_id_by_name[name] = None
                    return None
                print '\n\nupdate'
                for key, value in response_dict.iteritems():
                    print '{} = {}'.format(key, value)
            else:
                assert response.code == 200
                response_dict = json.loads(response.read())
                assert response_dict['success'] is True
#                    updated_group = response_dict['result']
#                    pprint.pprint(updated_group)
        else:
            print '\n\ncreate'
            for key, value in response_dict.iteritems():
                print '{} = {}'.format(key, value)
            raise
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
        request = urllib2.Request('http://ckan.quoi-ou.fr/api/3/action/organization_show?id={}'.format(name))
        request.add_header('Authorization', args.user_api_key)
        response = urllib2.urlopen(request)
        response_dict = json.loads(response.read())
        existing_organization = response_dict['result']

        organization['id'] = existing_organization['id']
        request = urllib2.Request('http://ckan.quoi-ou.fr/api/3/action/organization_update?id={}'.format(name))
        request.add_header('Authorization', args.user_api_key)
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
#                    updated_organization = response_dict['result']
#                    pprint.pprint(updated_organization)
    else:
        request = urllib2.Request('http://ckan.quoi-ou.fr/api/3/action/organization_create')
        request.add_header('Authorization', args.user_api_key)
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
