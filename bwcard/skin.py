"""Minecraftスキン（64x64のPNG）から立体的な全身・頭の画像を作る内蔵レンダラー。

外部のスキン描画サイトに頼らず、Mojang公式のスキン画像だけで描けます。
"""
from __future__ import annotations

import io
import math
import random

from PIL import Image, ImageChops, ImageDraw, ImageOps


# ---------------------------------------------------------------- スキンの読み込み
def load_skin(data: bytes | Image.Image) -> Image.Image:
    img = data if isinstance(data, Image.Image) else Image.open(io.BytesIO(data))
    img = img.convert("RGBA")
    if img.size == (64, 32):
        img = _upgrade_legacy(img)
    if img.size != (64, 64):
        img = img.resize((64, 64), Image.NEAREST)
    return img


def _upgrade_legacy(old: Image.Image) -> Image.Image:
    """旧形式(64x32)を新形式(64x64)に。左腕・左脚は右側を反転して作る。"""
    hat = old.crop((32, 0, 64, 16))
    if hat.getextrema()[3][0] == 255:  # 帽子レイヤーが全面不透明なら透明扱い（本家と同じ）
        old = old.copy()
        old.paste((0, 0, 0, 0), (32, 0, 64, 16))
    new = Image.new("RGBA", (64, 64), (0, 0, 0, 0))
    new.paste(old, (0, 0))

    def mirror(src, dst):
        sx, sy, w, h = src
        new.paste(ImageOps.mirror(old.crop((sx, sy, sx + w, sy + h))), dst)

    for bx, by, dx, dy in ((0, 16, 16, 48), (40, 16, 32, 48)):  # 脚, 腕
        mirror((bx + 4, by, 4, 4), (dx + 4, dy))          # 上
        mirror((bx + 8, by, 4, 4), (dx + 8, dy))          # 下
        mirror((bx + 0, by + 4, 4, 12), (dx + 8, dy + 4))  # 右側面→左側面
        mirror((bx + 4, by + 4, 4, 12), (dx + 4, dy + 4))  # 前
        mirror((bx + 8, by + 4, 4, 12), (dx + 0, dy + 4))  # 左側面→右側面
        mirror((bx + 12, by + 4, 4, 12), (dx + 12, dy + 4))  # 後ろ
    return new


# ---------------------------------------------------------------- 3D描画
# (名前, 最小座標xyz, サイズwhd, テクスチャ位置uv, 膨らませ量, 重ね着レイヤーか)
def _boxes(slim: bool):
    aw = 3 if slim else 4
    return [
        ("leg_r", (-4, 0, -2), (4, 12, 4), (0, 16), 0, False),
        ("leg_r2", (-4, 0, -2), (4, 12, 4), (0, 32), 0.25, True),
        ("leg_l", (0, 0, -2), (4, 12, 4), (16, 48), 0, False),
        ("leg_l2", (0, 0, -2), (4, 12, 4), (0, 48), 0.25, True),
        ("body", (-4, 12, -2), (8, 12, 4), (16, 16), 0, False),
        ("body2", (-4, 12, -2), (8, 12, 4), (16, 32), 0.25, True),
        ("arm_r", (-4 - aw, 12, -2), (aw, 12, 4), (40, 16), 0, False),
        ("arm_r2", (-4 - aw, 12, -2), (aw, 12, 4), (40, 32), 0.25, True),
        ("arm_l", (4, 12, -2), (aw, 12, 4), (32, 48), 0, False),
        ("arm_l2", (4, 12, -2), (aw, 12, 4), (48, 48), 0.25, True),
        ("head", (-4, 24, -4), (8, 8, 8), (0, 0), 0, False),
        ("head2", (-4, 24, -4), (8, 8, 8), (32, 0), 0.5, True),
    ]


def _faces(origin, size, uv, inflate):
    (x0, y0, z0), (w, h, d), (u, v), e = origin, size, uv, inflate
    x0, y0, z0 = x0 - e, y0 - e, z0 - e
    x1, y1, z1 = x0 + w + 2 * e, y0 + h + 2 * e, z0 + d + 2 * e
    # (法線, 原点, u方向の辺, v方向の辺, テクスチャ矩形)
    return [
        ((0, 0, 1), (x0, y1, z1), (x1 - x0, 0, 0), (0, y0 - y1, 0), (u + d, v + d, w, h)),           # 前
        ((0, 0, -1), (x1, y1, z0), (x0 - x1, 0, 0), (0, y0 - y1, 0), (u + 2 * d + w, v + d, w, h)),  # 後
        ((-1, 0, 0), (x0, y1, z0), (0, 0, z1 - z0), (0, y0 - y1, 0), (u, v + d, d, h)),              # 右側面
        ((1, 0, 0), (x1, y1, z1), (0, 0, z0 - z1), (0, y0 - y1, 0), (u + d + w, v + d, d, h)),       # 左側面
        ((0, 1, 0), (x0, y1, z0), (x1 - x0, 0, 0), (0, 0, z1 - z0), (u + d, v, w, d)),               # 上
        ((0, -1, 0), (x0, y0, z1), (x1 - x0, 0, 0), (0, 0, z0 - z1), (u + d + w, v, w, d)),          # 下
    ]


_SHADE = {(0, 1, 0): 1.0, (0, 0, 1): 0.86, (0, 0, -1): 0.6, (-1, 0, 0): 0.7, (1, 0, 0): 0.7, (0, -1, 0): 0.5}


def render(skin: Image.Image, height: int, slim: bool = False, yaw: float = 28, pitch: float = 14,
           parts: str = "body", overlay: bool = True, supersample: int = 2) -> Image.Image:
    """立体レンダリング。height = 出力画像の高さ(px)。parts = 'body'（全身）か 'head'（頭だけ）。"""
    ss = max(1, int(supersample))
    cy_, sy_ = math.cos(math.radians(yaw)), math.sin(math.radians(yaw))
    cp, sp = math.cos(math.radians(pitch)), math.sin(math.radians(pitch))

    def proj(p):
        x, y, z = p
        zr = -x * sy_ + z * cy_
        return (x * cy_ + z * sy_, -y * cp + zr * sp), zr * cp + y * sp

    boxes = [b for b in _boxes(slim) if (parts == "body" or b[0].startswith("head"))
             and (overlay or not b[5])]

    jobs = []  # 奥から順に描く（画家のアルゴリズム）
    for order, (name, origin, size, uv, inflate, is_overlay) in enumerate(boxes):
        center = tuple(origin[i] + size[i] / 2 for i in range(3))
        depth = proj(center)[1] + (0.001 if is_overlay else 0)
        for n, o, eu, ev, tex in _faces(origin, size, uv, inflate):
            if (-n[0] * sy_ + n[2] * cy_) * cp + n[1] * sp <= 1e-6:
                continue  # こちらを向いていない面
            jobs.append((depth, order, n, o, eu, ev, tex, is_overlay))
    jobs.sort(key=lambda j: (j[0], j[1]))

    def add(p, v, t=1.0):
        return (p[0] + v[0] * t, p[1] + v[1] * t, p[2] + v[2] * t)

    pts = [proj(add(add(o, eu, a), ev, b))[0] for _, _, _, o, eu, ev, _, _ in jobs
           for a, b in ((0, 0), (1, 0), (0, 1), (1, 1))]
    minx, maxx = min(p[0] for p in pts), max(p[0] for p in pts)
    miny, maxy = min(p[1] for p in pts), max(p[1] for p in pts)
    HB = height * ss
    pad = 2 * ss
    k = (HB - 2 * pad) / (maxy - miny)
    WB = math.ceil((maxx - minx) * k) + 2 * pad
    out = Image.new("RGBA", (WB, HB), (0, 0, 0, 0))

    def scr(p):
        (sx, sy), _ = proj(p)
        return (sx - minx) * k + pad, (sy - miny) * k + pad

    grow = 0.6 * ss  # 面と面の継ぎ目に隙間ができないよう少しだけ外側まで描く
    for _, _, n, o, eu, ev, (tu, tv, tw, th), is_overlay in jobs:
        face = skin.crop((tu, tv, tu + tw, tv + th))
        if not is_overlay:
            face.putalpha(255)
        elif face.getextrema()[3][1] == 0:
            continue
        shade = _SHADE.get(n, 0.8)
        if shade < 1:
            r, g, b, a = face.split()
            r, g, b = (ch.point(lambda v: int(v * shade)) for ch in (r, g, b))
            face = Image.merge("RGBA", (r, g, b, a))
        # 端の色を1ドット外側にも複製（隙間埋め用）
        padded = Image.new("RGBA", (tw + 2, th + 2))
        padded.paste(face, (1, 1))
        padded.paste(face.crop((0, 0, tw, 1)), (1, 0))
        padded.paste(face.crop((0, th - 1, tw, th)), (1, th + 1))
        padded.paste(padded.crop((1, 0, 2, th + 2)), (0, 0))
        padded.paste(padded.crop((tw, 0, tw + 1, th + 2)), (tw + 1, 0))

        O, P1, P2 = scr(o), scr(add(o, eu)), scr(add(o, ev))
        U = ((P1[0] - O[0]) / tw, (P1[1] - O[1]) / tw)
        V = ((P2[0] - O[0]) / th, (P2[1] - O[1]) / th)
        P3 = (P1[0] + P2[0] - O[0], P1[1] + P2[1] - O[1])
        quad = [O, P1, P3, P2]
        bx = math.floor(min(q[0] for q in quad) - grow)
        by = math.floor(min(q[1] for q in quad) - grow)
        ex = math.ceil(max(q[0] for q in quad) + grow)
        ey = math.ceil(max(q[1] for q in quad) + grow)
        det = U[0] * V[1] - V[0] * U[1]
        if abs(det) < 1e-9:
            continue
        a, b, d, e = V[1] / det, -V[0] / det, -U[1] / det, U[0] / det
        c = a * (bx - O[0]) + b * (by - O[1]) + 1
        f = d * (bx - O[0]) + e * (by - O[1]) + 1
        warped = padded.transform((ex - bx, ey - by), Image.AFFINE, (a, b, c, d, e, f),
                                  resample=Image.NEAREST)
        cx = sum(q[0] for q in quad) / 4
        cy = sum(q[1] for q in quad) / 4
        poly = []
        for qx, qy in quad:
            vx, vy = qx - cx, qy - cy
            ln = math.hypot(vx, vy) or 1
            poly.append((qx + vx / ln * grow - bx, qy + vy / ln * grow - by))
        clip = Image.new("L", warped.size, 0)
        ImageDraw.Draw(clip).polygon(poly, fill=255)
        warped.putalpha(ImageChops.multiply(warped.getchannel("A"), clip))
        out.alpha_composite(warped, (bx, by))
    if ss > 1:
        out = out.resize((max(1, round(WB / ss)), height), Image.BOX)
    return out


# ---------------------------------------------------------------- 代わりのスキン
def silhouette_skin() -> Image.Image:
    """スキンが取れなかったとき用の無地スキン（顔なし）。"""
    img = Image.new("RGBA", (64, 64), (0, 0, 0, 0))
    regions = [((0, 0, 32, 16), (150, 156, 168)), ((16, 16, 40, 32), (120, 126, 140)),
               ((40, 16, 56, 32), (132, 138, 150)), ((32, 48, 48, 64), (132, 138, 150)),
               ((0, 16, 16, 32), (98, 104, 118)), ((16, 48, 32, 64), (98, 104, 118))]
    for box, col in regions:
        img.paste(col + (255,), box)
    return img


def sample_skin(seed: int = 7) -> Image.Image:
    """プレビュー用のオリジナルスキン（パーカーとヘッドホンのキャラ）。"""
    rnd = random.Random(seed)
    img = Image.new("RGBA", (64, 64), (0, 0, 0, 0))
    px = img.load()

    def jitter(c, amt=10):
        d = rnd.randint(-amt, amt)
        return tuple(max(0, min(255, v + d)) for v in c) + (255,)

    def fill(x0, y0, w, h, col, amt=8):
        for y in range(y0, y0 + h):
            for x in range(x0, x0 + w):
                px[x, y] = jitter(col, amt)

    SKIN, HAIR, HAIR2 = (241, 200, 170), (64, 46, 58), (88, 64, 80)
    HOOD, HOOD2, ZIP = (92, 74, 214), (70, 56, 178), (210, 214, 236)
    JEANS, SHOE, SOLE = (52, 72, 116), (236, 236, 240), (80, 80, 92)
    PHONE, PHONE2 = (255, 112, 160), (220, 70, 120)

    # 頭（顔は正面 8..16, 8..16）
    fill(8, 0, 8, 8, HAIR)            # 上
    fill(16, 0, 8, 8, SKIN, 4)        # 下
    for fx in (0, 16):                # 両側面
        fill(fx, 8, 8, 4, HAIR)
        fill(fx, 12, 8, 4, SKIN, 4)
        fill(fx + 1, 12, 3, 4, HAIR2)
    fill(24, 8, 8, 8, HAIR)           # 後ろ
    fill(8, 8, 8, 8, SKIN, 4)         # 顔
    fill(8, 8, 8, 2, HAIR)
    for x in (8, 9, 12, 14, 15):
        px[x, 10] = jitter(HAIR2, 6)
    for x, y, c in ((9, 12, (250, 250, 255)), (10, 12, (60, 110, 220)), (13, 12, (60, 110, 220)),
                    (14, 12, (250, 250, 255)), (10, 13, (40, 80, 180)), (13, 13, (40, 80, 180)),
                    (11, 14, (210, 140, 130)), (12, 14, (210, 140, 130))):
        px[x, y] = c + (255,)
    # ヘッドホン（帽子レイヤー）
    fill(40, 3, 8, 2, PHONE)          # 上のバンド
    fill(32, 10, 8, 5, (0, 0, 0), 0)
    for fx in (32, 48):
        fill(fx + 2, 10, 4, 5, PHONE)
        fill(fx + 3, 11, 2, 3, PHONE2)
        fill(fx + 3, 8, 2, 2, PHONE)
    for x in range(32, 64):
        for y in range(0, 16):
            if px[x, y][:3] == (0, 0, 0) or px[x, y][3] == 0:
                px[x, y] = (0, 0, 0, 0)

    # 胴体（パーカー）
    fill(20, 16, 8, 4, HOOD); fill(28, 16, 8, 4, HOOD2)
    fill(16, 20, 4, 12, HOOD2); fill(28, 20, 4, 12, HOOD2)
    fill(20, 20, 8, 12, HOOD); fill(32, 20, 8, 12, HOOD2)
    fill(23, 20, 2, 12, ZIP, 6)
    for x in (22, 25):
        for y in range(20, 25):
            px[x, y] = (245, 245, 250, 255)
    fill(21, 28, 6, 2, HOOD2)
    fill(32, 20, 8, 3, (58, 46, 150))
    # 腕
    for (ax, ay) in ((40, 16), (32, 48)):
        fill(ax + 4, ay, 4, 4, HOOD); fill(ax + 8, ay, 4, 4, SKIN, 4)
        for i, col in enumerate((HOOD2, HOOD, HOOD2, HOOD2)):
            fill(ax + i * 4, ay + 4, 4, 9, col)
            fill(ax + i * 4, ay + 13, 4, 1, (120, 104, 230))
            fill(ax + i * 4, ay + 14, 4, 2, SKIN, 4)
    # 脚
    for (lx, ly) in ((0, 16), (16, 48)):
        fill(lx + 4, ly, 4, 4, JEANS); fill(lx + 8, ly, 4, 4, SOLE)
        for i in range(4):
            fill(lx + i * 4, ly + 4, 4, 9, JEANS)
            fill(lx + i * 4, ly + 13, 4, 2, SHOE, 4)
            fill(lx + i * 4, ly + 15, 4, 1, SOLE, 4)
    return img
