from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional
import jinja2

_DEFAULT_TEMPLATES_DIR = Path(__file__).resolve().parent


class TemplateEngine:
    """Jinja2-based template registry and rendering engine for code generation."""

    _instance: Optional[TemplateEngine] = None

    def __init__(self, templates_dir: Optional[str | Path] = None):
        self.templates_dir = Path(templates_dir or _DEFAULT_TEMPLATES_DIR)
        self.env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(str(self.templates_dir)),
            autoescape=False,
            trim_blocks=True,
            lstrip_blocks=True,
            undefined=jinja2.Undefined,
        )

    @classmethod
    def get_instance(cls) -> TemplateEngine:
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def render(self, template_path: str, **kwargs: Any) -> str:
        """Renders a template file located within the templates_dir."""
        # Normalize template path for cross-platform forward slash
        normalized_path = template_path.replace("\\", "/")
        tmpl = self.env.get_template(normalized_path)
        return tmpl.render(**kwargs)

    def render_string(self, template_str: str, **kwargs: Any) -> str:
        """Renders raw Jinja2 template string directly."""
        tmpl = self.env.from_string(template_str)
        return tmpl.render(**kwargs)
