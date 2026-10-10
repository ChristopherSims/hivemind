import pytest

from hivemind.hooks import mutation_paths


@pytest.mark.parametrize("field", ["raw", "patch", "input"])
def test_patch_guard_collects_every_file_and_move_destination(field):
    patch = """*** Begin Patch
*** Update File: src/header/old.ts
*** Move to: src/header/new.ts
@@
-old
+new
*** Add File: src/header/added.ts
+created
*** Delete File: src/header/deleted.ts
*** End Patch"""
    value = patch if field == "raw" else {field: patch}
    assert mutation_paths("ApplyPatch", value) == [
        "src/header/old.ts", "src/header/new.ts", "src/header/added.ts", "src/header/deleted.ts"]


@pytest.mark.parametrize("value", [
    "not a patch", "*** Begin Patch\n*** Execute: arbitrary command\n*** End Patch",
    "*** Begin Patch\n*** End Patch", {"unknown": "patch"},
])
def test_unknown_patch_shapes_fail_closed(value):
    with pytest.raises(RuntimeError):
        mutation_paths("ApplyPatch", value)
