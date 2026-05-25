#!/usr/bin/env python
# -*- coding: utf-8 -*- #

AUTHOR = "NYC DSA Healthcare"
SITENAME = "NYC DSA Healthcare Working Group"
SITEURL = ""

PATH = "content"

TIMEZONE = "America/New_York"

DEFAULT_LANG = "en"

# Feed generation is usually not desired when developing
FEED_ALL_ATOM = None
CATEGORY_FEED_ATOM = None
TRANSLATION_FEED_ATOM = None
AUTHOR_FEED_ATOM = None
AUTHOR_FEED_RSS = None

# Blogroll
LINKS = (
    ("NYC DSA", "https://nycdsa.org/"),
    ("DSA National", "https://dsausa.org/"),
)

# Social widget
SOCIAL = (
    ("Twitter", "https://twitter.com/nycdsa"),
    ("GitHub", "https://github.com/nyc-dsa-healthcare"),
)

DEFAULT_PAGINATION = 10

# Theme
THEME = "theme/dsa-theme"

# Plugins
PLUGIN_PATHS = ["plugins"]
PLUGINS = ["action_network"]

# Donate link — used in nav and index page donate strip
DONATE_URL = "https://chuffed.org/project/nyc-dsa-healthcare"

# SEO defaults
SITE_DESCRIPTION = "NYC DSA's Healthcare Working Group is building a mass movement for single-payer healthcare in New York. Find free healthcare resources, take action, and join the fight for the NY Health Act."
SITE_IMAGE = "https://healthcare.socialists.nyc/theme/images/nycdsa-hcwg-logo.png"

# Uncomment following line if you want document-relative URLs when developing
# RELATIVE_URLS = True
