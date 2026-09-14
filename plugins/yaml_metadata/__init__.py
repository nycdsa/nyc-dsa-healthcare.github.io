"""
Pelican plugin: YAML front matter support for Markdown files.

Decap CMS writes YAML front matter (--- delimited blocks with lowercase keys).
Pelican's default MarkdownReader expects its own key: value format.
This plugin bridges the gap by converting YAML front matter before Pelican reads it.
"""

import os
import re
import tempfile

from pelican import signals
from pelican.readers import MarkdownReader


YAML_FM_RE = re.compile(r'\A---\s*\n(.*?)\n---\s*\n', re.DOTALL)


class YamlMetadataMarkdownReader(MarkdownReader):
    """Reads Markdown files with either YAML (---) or native Pelican front matter."""

    def read(self, source_path):
        with open(source_path, encoding='utf-8') as f:
            raw = f.read()

        match = YAML_FM_RE.match(raw)
        if not match:
            return super().read(source_path)

        try:
            import yaml
        except ImportError:
            # PyYAML not available — fall back to default reader
            return super().read(source_path)

        yaml_block = match.group(1)
        body = raw[match.end():]

        raw_meta = yaml.safe_load(yaml_block) or {}

        # Build a Pelican-native metadata block (key: value, no --- delimiters)
        meta_lines = []
        for key, value in raw_meta.items():
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            # Convert date/datetime objects to YYYY-MM-DD string
            if hasattr(value, 'isoformat'):
                value = value.isoformat()[:10]
            else:
                value = str(value)
            meta_lines.append(f'{key}: {value}')

        converted = '\n'.join(meta_lines) + '\n\n' + body

        # Write to a temp file and let the parent MarkdownReader handle it
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.md', encoding='utf-8', delete=False
        ) as tmp:
            tmp.write(converted)
            tmp_path = tmp.name

        try:
            return super().read(tmp_path)
        finally:
            os.unlink(tmp_path)


def add_reader(readers):
    for ext in ('md', 'markdown', 'mkd', 'mdown'):
        readers.reader_classes[ext] = YamlMetadataMarkdownReader


def register():
    signals.readers_init.connect(add_reader)
