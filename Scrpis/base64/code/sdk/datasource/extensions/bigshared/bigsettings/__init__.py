import json
import os


def settings_from_env(settings_name, settings):
    if "." not in settings_name:
        raise Exception("Invalid settings module name %s. Please import it with full package name" % settings_name)

    settings_prefix = settings_name.replace(".", "__")
    # print('load settings from env for %s' % settings_prefix)
    for key, value in os.environ.items():
        if not key.startswith(settings_prefix):
            continue
        key = key[len(settings_prefix) + 2:]
        if not key or "__" in key:
            continue
        if key in settings and settings[key] is not None and not isinstance(settings[key], str):
            try:
                value = json.loads(value)
            except Exception:
                import ast

                value = ast.literal_eval(value)
        settings[key] = value
        # print('overwrite settings: %s.%s=%s' % (settings_name, key, value))
