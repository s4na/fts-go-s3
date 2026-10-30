import json
import pathlib
import sys

comparison = pathlib.Path(sys.argv[1]).read_text(encoding='utf-8')
print(json.dumps({'body': '<!-- zenbun-metrics -->\n' + comparison}, ensure_ascii=False))
