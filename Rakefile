require 'rake'
require 'rake/testtask'

PRJ = "spinoza"

def version
  @version ||= begin
    require 'spinoza/version'
    warn "Spinoza::VERSION not a string" unless Spinoza::VERSION.kind_of? String
    Spinoza::VERSION
  end
end

def tag
  @tag ||= "#{PRJ}-#{version}"
end

desc "Run all tests"
task :test => %w{ test:unit }

namespace :test do
  desc "Run unit tests"
  Rake::TestTask.new :unit do |t|
    t.libs << "lib"
    t.test_files = FileList["test/*.rb"]
  end
end

desc "Commit, tag, and push repo; build and push gem"
task :release => "release:is_new_version" do
  require 'tempfile'
  
  sh "gem build #{PRJ}.gemspec"

  file = Tempfile.new "template"
  begin
    file.puts "release #{version}"
    file.close
    sh "git commit --allow-empty -a -v -t #{file.path}"
  ensure
    file.close unless file.closed?
    file.unlink
  end

  sh "git tag #{tag}"
  sh "git push"
  sh "git push --tags"
  
  sh "gem push #{tag}.gem"
end

namespace :release do
  desc "Diff to latest release"
  task :diff do
    latest = `git describe --abbrev=0 --tags --match '#{PRJ}-*'`.chomp
    sh "git diff #{latest}"
  end

  desc "Log to latest release"
  task :log do
    latest = `git describe --abbrev=0 --tags --match '#{PRJ}-*'`.chomp
    sh "git log #{latest}.."
  end

  task :is_new_version do
    abort "#{tag} exists; update version!" unless `git tag -l #{tag}`.empty?
  end
end
