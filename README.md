Pithos.io
---------

This is the jekyll/github pages powered website for the Pithos project.

To run a local copy:

```
npm install
npm install -g grunt-cli
gem install bundle
bundle install
```

Automatically rebuild the website as you make changes:

```
grunt watch
```

Serve the site locally:

```
cd _site
python -m http.server  # Or '-m SimpleHTTPServer' if you only have python 2
```

Long text and pages should go in the `_includes`directory and
be render as Markdown.
