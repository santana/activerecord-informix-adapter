require 'rubygems'
require 'rake'
require 'rake/gempackagetask'

PKG_NAME = 'activerecord-informix-adapter'
PKG_VERSION = "1.1.1"

spec = Gem::Specification.new do |s|
  s.name = PKG_NAME
  s.summary = 'Informix adapter for Active Record'
  s.version = PKG_VERSION

  s.add_dependency 'activerecord', '>= 1.15.4.7707'
  s.add_dependency 'ruby-informix', '>= 0.7.3'
  s.require_path = 'lib'

  s.files = %w(lib/active_record/connection_adapters/informix_adapter.rb)

  s.author = 'Gerardo Santana Gomez Garrido'
  s.email = 'gerardo.santana@gmail.com'
  s.homepage = 'http://rails-informix.rubyforge.org/'
  s.rubyforge_project = 'rails-informix'
end

Rake::GemPackageTask.new(spec) do |p|
  p.gem_spec = spec
  p.need_tar = true
  p.need_zip = true
end
