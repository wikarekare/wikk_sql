# -*- ruby -*-
require 'rubygems'
require 'hoe'
Hoe.plugin :yard

Hoe.spec 'wikk_sql' do
  self.readme_file = 'README.md'
  self.developer( 'Rob Burrowes', 'r.burrowes@auckland.ac.nz')
  remote_rdoc_dir = '' # Release to root

  self.yard_title = 'wikk_sql'
  self.yard_options = [ '--markup', 'markdown', '--protected' ]

  #  self.dependency "ruby-mysql", ['~> 1.0', '>= 1.0.0']
end

# Validate manfest.txt
# rake check_manifest

# Local checking. Creates pkg/
# rake gem

# create doc/
# rake docs

# Copy up to rubygem.org
# rake release VERSION=1.0.1
