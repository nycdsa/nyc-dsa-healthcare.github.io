#!/usr/bin/env python
# -*- coding: utf-8 -*- #

AUTHOR = "NYC DSA Healthcare"
SITENAME = "NYC DSA Healthcare Working Group"
SITEURL = ""

PATH = "content"

STATIC_PATHS = ["images", "data", "admin"]

# Prevent Pelican from trying to process the CMS admin files as articles/pages
ARTICLE_EXCLUDES = ["admin"]
PAGE_EXCLUDES = ["admin"]

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
PLUGINS = ["action_network", "donations", "yaml_metadata"]

# Donate link — used in nav and index page donate strip
DONATE_URL = "https://chuffed.org/project/nyc-dsa-healthcare"

# Featured series — shown as a row of boxes at the top of the blog page.
# Posted parts link by slug; parts without a slug show as "coming soon" with their note.
# Set BLOG_SERIES = None to hide the section.
BLOG_SERIES = {
    "title": "NYC-DSA 2026 Citywide Convention",
    "description": "Arguments surrounding the Healthcare For All resolution from the authors of the original resolution and the authors of each of the two proposed amendments.",
    "parts": [
        {"slug": "convention-2026-hcfa-response-to-amendments"},
        {"slug": "convention-2026-building-organizing-power-amendment"},
        {
            "title": "Rationale Behind the Amendment: For a Fighting & Organization Building Campaign",
            "author": "Authors of the For a Fighting & Organization Building Campaign Amendment",
            "note": "To be posted Thursday, October 29",
        },
    ],
}

# SEO defaults
SITE_DESCRIPTION = "NYC DSA's Healthcare Working Group is building a mass movement for single-payer healthcare in New York. Find free healthcare resources, take action, and join the fight for the NY Health Act."
SITE_IMAGE = "https://healthcare.socialists.nyc/theme/images/favicon.png"

# Uncomment following line if you want document-relative URLs when developing
# RELATIVE_URLS = True
