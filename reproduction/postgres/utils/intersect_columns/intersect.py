import sys


def file_to_line_set(filename: str) -> set[str]:
    with open(filename, "r") as f:
        return set(f)


out: set[str] | None = None
for file in sys.argv[1:]:
    if out is None:
        out = file_to_line_set(file)
    else:
        out &= file_to_line_set(file)
if out:
    print("".join(sorted(out)), end="")
