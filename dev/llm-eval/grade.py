"""Сводка прогонов out/<tag>-s<N>-r<R>.json: время, шаги, ошибки инструментов и эвристики по ключу ответов.

python3 -I grade.py <tag> [<tag> ...]
Ключ: s1 — payments, min.insync.replicas > RF; s2 — у группы нет участников (потребитель не запущен);
s3 — payment_failed заказа 123 не обработан. Оффсеты 481 и 181 — от seed.sh на чистом стенде
(партиция 2): после другого заполнения проверьте их по ответу вручную.
"""
import glob
import json
import re
import sys


def flags(scn, ans):
    a = (ans or "").lower()
    if scn == "s1":
        return {"payments+minISR": "payments" in a and "min.insync" in a}
    if scn == "s2":
        return {"no members": bool(re.search(r"empty|нет (активных )?(участник|член|потребител|консьюмер)|members: \[\]|не запущен", a))}
    return {
        "payment_failed": "payment_failed" in a,
        "offset 481": "481" in a,
        "committed 181": "181" in a,
        "says unprocessed": bool(re.search(r"не (был[оа]? )?обработ|не успел|не обработан", a)),
    }


for tag in sys.argv[1:]:
    print(f"\n######## {tag}")
    for scn in ("s1", "s2", "s3"):
        for f in sorted(glob.glob(f"out/{tag}-{scn}-r*.json")):
            d = json.load(open(f))
            errs = sum(c["result_head"].startswith("ERROR") for s in d["steps"] for c in s["calls"])
            first = d["steps"][0]["llm_s"] if d["steps"] else None
            compl = sum((s.get("usage") or {}).get("completion_tokens") or 0 for s in d["steps"])
            llm = sum(s["llm_s"] for s in d["steps"])
            print(f"{f.split('/')[-1]:22s} {d['total_s']:6.1f}s steps {len(d['steps']):2d} tool_err {errs} "
                  f"first {first}s tok/s~{compl / llm if llm else 0:.0f} stopped={d.get('stopped')} {flags(scn, d.get('answer'))}")
