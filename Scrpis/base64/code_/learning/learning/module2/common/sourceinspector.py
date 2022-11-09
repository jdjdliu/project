import inspect
import re
import sys


def bq_protected_inspect_findsource(obj, loader):
    source = loader.get_source(obj.__module__)
    lines = source and source.splitlines(True)
    if not lines:
        raise OSError('could not get source code')
    if not lines[-1].endswith('\n'):
        lines[-1] += '\n'

    if inspect.ismodule(obj):
        return lines, 0

    if inspect.isclass(obj):
        name = obj.__name__
        pat = re.compile(r'^(\s*)class\s*' + name + r'\b')
        # make some effort to find the best matching class definition:
        # use the one with the least indentation, which is the one
        # that's most probably not inside a function definition.
        candidates = []
        for i in range(len(lines)):
            match = pat.match(lines[i])
            if match:
                # if it's at toplevel, it's already the best one
                if lines[i][0] == 'c':
                    return lines, i
                # else add whitespace to candidate list
                candidates.append((match.group(1), i))
        if candidates:
            # this will sort by whitespace, and by line number,
            # less whitespace first
            candidates.sort()
            return lines, candidates[0][1]
        else:
            raise OSError('could not find class definition')

    if inspect.ismethod(obj):
        obj = obj.__func__
    if inspect.isfunction(obj):
        obj = obj.__code__
    if inspect.istraceback(obj):
        obj = obj.tb_frame
    if inspect.isframe(obj):
        obj = obj.f_code
    if inspect.iscode(obj):
        if not hasattr(obj, 'co_firstlineno'):
            raise OSError('could not find function definition')
        lnum = obj.co_firstlineno - 1
        pat = re.compile(
            r'^(\s*def\s)|(\s*async\s+def\s)|(.*(?<!\w)lambda(:|\s))|^(\s*@)')
        if lnum >= len(lines):
            return lines, -1
        while lnum >= 0:
            if pat.match(lines[lnum]):
                break
            lnum = lnum - 1
        return lines, lnum
    raise OSError('could not find code obj')


def bq_protected_inspect_getsourcelines(obj, source_provider):
    obj = inspect.unwrap(obj)
    lines, lnum = bq_protected_inspect_findsource(obj, source_provider)

    if inspect.ismodule(obj):
        return lines, 0
    elif lnum < 0:
        return lines, lnum
    else:
        return inspect.getblock(lines[lnum:]), lnum + 1


def bq_protected_is_cython_function(obj):
    return getattr(type(obj), '__name__', None) == 'cython_function_or_method'


def bq_protected_sig_for_cythonized(obj):
    s = str(obj)
    pos = s.find(' at ')
    if pos != -1:
        s = s[:pos]
    return s


def bq_protected_inspect_getsource(obj, sig_for_cythonized=False):
    try:
        lines, lnum = bq_protected_inspect_getsourcelines(
            obj, sys.modules[obj.__module__].__loader__)
        if lnum < 0:
            import hashlib
            co_code_md5 = hashlib.md5(obj.__code__.co_code).hexdigest()
            return co_code_md5
        return ''.join(lines)
    except Exception:
        try:
            return inspect.getsource(obj)
        except Exception:
            # TODO cythonized?
            if not sig_for_cythonized:
                raise
            return bq_protected_sig_for_cythonized(obj)
