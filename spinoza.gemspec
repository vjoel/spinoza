require 'spinoza/version'

Gem::Specification.new do |s|
  s.name = "spinoza"
  s.version = Spinoza::VERSION

  s.required_rubygems_version = Gem::Requirement.new(">= 0")
  s.authors = ["Joel VanderWerf"]
  s.date = Time.now.strftime "%Y-%m-%d"
  s.summary = "Model of Calvin distributed database."
  s.description = "Model of Calvin distributed database."
  s.email = "vjoel@users.sourceforge.net"
  s.extra_rdoc_files = ["README.md", "COPYING"]
  s.files = Dir[
    "README.md", "COPYING", "Rakefile",
    "lib/**/*.rb",
    "bin/**/*.rb",
    "bench/**/*.rb",
    "bugs/**/*.rb",
    "example/**/*.rb",
    "test/**/*.rb"
  ]
  s.bindir = 'bin'
  s.test_files = Dir["test/*.rb"]
  s.homepage = "https://github.com/vjoel/spinoza"
  s.license = "BSD"
  s.rdoc_options = [
    "--quiet", "--line-numbers", "--inline-source",
    "--title", "spinoza", "--main", "README.md"]
  s.require_paths = ["lib"]

  s.required_ruby_version = Gem::Requirement.new("~> 2.0")
  s.add_runtime_dependency 'sequel', '~> 0'
  s.add_runtime_dependency 'sqlite3', '~> 0'
end
