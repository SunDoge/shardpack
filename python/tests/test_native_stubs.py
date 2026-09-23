"""Keep hand-maintained native signatures synchronized with PyO3 exports."""

import ast
import inspect
from pathlib import Path

from shardpack import _native


def test_native_stub_signatures_match_extension():
    stub = Path(_native.__file__).with_name("_native.pyi")
    tree = ast.parse(stub.read_text())
    exports = {node.name for node in tree.body if isinstance(node, (ast.ClassDef, ast.FunctionDef))}
    assert exports == {name for name in dir(_native) if not name.startswith("_")}
    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            functions = [(node, getattr(_native, node.name))]
        elif isinstance(node, ast.ClassDef):
            cls = getattr(_native, node.name)
            functions = []
            for method in node.body:
                if not isinstance(method, ast.FunctionDef):
                    continue
                if any(
                    isinstance(d, ast.Name) and d.id == "property" for d in method.decorator_list
                ):
                    assert hasattr(cls, method.name)
                elif method.name == "__init__":
                    functions.append((method, cls))
                elif not method.name.startswith("__"):
                    functions.append((method, getattr(cls, method.name)))
        else:
            continue
        for definition, native in functions:
            arguments = definition.args
            positional = arguments.posonlyargs + arguments.args
            defaults = [None] * (len(positional) - len(arguments.defaults)) + arguments.defaults
            expected = []
            for index, (arg, default) in enumerate(zip(positional, defaults, strict=True)):
                kind = (
                    inspect.Parameter.POSITIONAL_ONLY
                    if index < len(arguments.posonlyargs)
                    else inspect.Parameter.POSITIONAL_OR_KEYWORD
                )
                if arg.arg != "self":
                    expected.append(
                        inspect.Parameter(
                            arg.arg,
                            kind,
                            default=inspect.Parameter.empty
                            if default is None
                            else ast.literal_eval(default),
                        )
                    )
            for arg, default in zip(arguments.kwonlyargs, arguments.kw_defaults, strict=True):
                expected.append(
                    inspect.Parameter(
                        arg.arg,
                        inspect.Parameter.KEYWORD_ONLY,
                        default=inspect.Parameter.empty
                        if default is None
                        else ast.literal_eval(default),
                    )
                )
            actual = [p for p in inspect.signature(native).parameters.values() if p.name != "self"]
            # PyO3 exposes Rust constant defaults as Ellipsis in text signatures.
            expected = [
                p.replace(default=Ellipsis) if a.default is Ellipsis else p
                for p, a in zip(expected, actual, strict=True)
            ]
            assert expected == actual, definition.name
