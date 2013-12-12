#! /usr/bin/env python
# -*- coding: utf-8 -*-


# Etalab-to-CKAN -- Tools to help migration of data.gouv.fr to CKAN
# By: Emmanuel Raviart <emmanuel@raviart.com>
#
# Copyright (C) 2013 Etalab
# http://github.com/etalab/etalab-to-ckan
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


"""Change the groups of the datasets, organization by organization."""


import argparse
import ConfigParser
import csv
import datetime
import json
import logging
import os
import sys
import urllib
import urllib2
import urlparse

from biryani1 import baseconv, custom_conv, states, strings

from ckantoolbox import ckanconv


app_name = os.path.splitext(os.path.basename(__file__))[0]
conv = custom_conv(baseconv, ckanconv, states)
log = logging.getLogger(app_name)


def main():
    parser = argparse.ArgumentParser(description = __doc__)
    parser.add_argument('config', help = 'path of configuration file')
    parser.add_argument('csv_file_path', help = 'path of CSV file containing the groups to use by organization')
    parser.add_argument('-v', '--verbose', action = 'store_true', help = 'increase output verbosity')

    global args
    args = parser.parse_args()
    logging.basicConfig(level = logging.DEBUG if args.verbose else logging.WARNING, stream = sys.stdout)

    config_parser = ConfigParser.SafeConfigParser(dict(
        here = os.path.dirname(os.path.abspath(os.path.normpath(args.config))),
        ))
    config_parser.read(args.config)
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
                },
            default = 'drop',
            ),
        conv.not_none,
        ))(dict(config_parser.items('Change-datasets-groups-by-organization')), conv.default_state)

    ckan_headers = {
        'Authorization': conf['ckan.api_key'],
        'User-Agent': conf['user_agent'],
        }

    group_by_name = {}
    groups_name_by_organization_name = {}
    organization_by_id = {}
    organization_by_name = {}
    with open(args.csv_file_path) as csv_file:
        csv_reader = csv.reader(csv_file)
        csv_reader.next()
        for row in csv_reader:
            organization_title, group1_title, group2_title = [
                cell.decode('utf-8').strip() or None
                for cell in row
                ]
            if organization_title is None or group1_title is None:
                continue
            organization_name = strings.slugify(organization_title)[:100]
            if organization_name not in organization_by_name:
                request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'],
                    '/api/3/action/organization_show?id={}'.format(organization_name)), headers = ckan_headers)
                try:
                    response = urllib2.urlopen(request)
                except urllib2.HTTPError as response:
                    if response.code == 404:
                        log.warning(u'Skipping missing organization: {}'.format(organization_name))
                        continue
                    raise
                else:
                    response_dict = json.loads(response.read())
                    organization = response_dict['result']
                    organization_by_id[organization['id']] = organization
                    organization_by_name[organization_name] = organization
            for group_title in (group1_title, group2_title):
                if group_title is None:
                    continue
                group_name = strings.slugify(group_title)[:100]
                if group_name not in group_by_name:
                    request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'],
                        '/api/3/action/group_show?id={}'.format(group_name)), headers = ckan_headers)
                    try:
                        response = urllib2.urlopen(request)
                    except urllib2.HTTPError as response:
                        if response.code == 404:
                            log.info(u'Creating group: {}'.format(group_name))
                            request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'],
                                '/api/3/action/group_create'), headers = ckan_headers)
                            response = urllib2.urlopen(request, urllib.quote(json.dumps(dict(
                                name = group_name,
                                title = group_title
                                ))))
                            response_dict = json.loads(response.read())
                            group_by_name[group_name] = response_dict['result']
                        else:
                            raise
                    else:
                        response_dict = json.loads(response.read())
                        group_by_name[group_name] = response_dict['result']
                groups_name_by_organization_name.setdefault(organization_name, set()).add(group_name)

    # Retrieve names of packages already existing in CKAN.
    request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'], '/api/3/action/package_list'),
        headers = ckan_headers)
    response = urllib2.urlopen(request)
    response_dict = json.loads(response.read())
    packages_name = conv.check(conv.pipe(
        conv.ckan_json_to_name_list,
        conv.not_none,
        ))(response_dict['result'], state = conv.default_state)

    for package_name in packages_name:
        request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'],
            '/api/3/action/package_show?id={}'.format(package_name)), headers = ckan_headers)
        response = urllib2.urlopen(request)
        response_dict = json.loads(response.read())
        package = conv.check(conv.pipe(
            conv.make_ckan_json_to_package(drop_none_values = True),
            conv.not_none,
            conv.ckan_input_package_to_output_package,
            ))(response_dict['result'], state = conv.default_state)
        organization_id = package.get('owner_org')
        organization = organization_by_id.get(organization_id)
        if organization is None:
            continue
        groups_name = set(
            group['name']
            for group in (package.get('groups') or [])
            )
        organization_groups_name = groups_name_by_organization_name[organization['name']]
        for group_name in organization_groups_name:
            if group_name not in groups_name:
                log.info(u'Adding group {} to package {}'.format(group_name, package['name']))
                request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'],
                    '/api/3/action/member_create'), headers = ckan_headers)
                response = urllib2.urlopen(request, urllib.quote(json.dumps(dict(
                    capacity = 'public',
                    id = group_name,
                    object = package['name'],
                    object_type = 'package',
                    ))))
                response_dict = json.loads(response.read())
        for group_name in groups_name:
            if group_name not in organization_groups_name:
                log.info(u'Removing group {} from package {}'.format(group_name, package['name']))
                request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'],
                    '/api/3/action/member_delete'), headers = ckan_headers)
                response = urllib2.urlopen(request, urllib.quote(json.dumps(dict(
                    id = group_name,
                    object = package['name'],
                    object_type = 'package',
                    ))))

    return 0


if __name__ == '__main__':
    sys.exit(main())
