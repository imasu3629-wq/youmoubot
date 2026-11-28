"""描画のための小道具。デザイン側は「論理座標（px）」で書き、scale 倍の実ピクセルで描きます。"""
from __future__ import annotations

import io
import math
from functools import lru_cache

from PIL import Image, ImageChops, ImageDraw, ImageFilter

from . import text as T

@lru_cache(maxsize=512)
def _rr_mask(w: int, h: int, r: int) -> Image.Image:
    """アンチエイリアス付き角丸マスク（4倍で描いて縮小）。"""
    ss = 4
    big = Image.new("L", (w * ss, h * ss), 0)
    ImageDraw.Draw(big).rounded_rectangle((0, 0, w * ss - 1, h * ss - 1), radius=r * ss, fill=255)
    return big.resize((w, h), Image.BOX)


def _rgba(c) -> tuple[int, int, int, int]:
    if isinstance(c, str):
        rgb = T.to_rgb(c)
        return rgb + (255,)
    return tuple(c) if len(c) == 4 else tuple(c) + (255,)


def lerp(a, b, t):
    return tuple(round(a[i] + (b[i] - a[i]) * t) for i in range(len(a)))


def mix(c, other, t):
    return lerp(_rgba(c), _rgba(other), t)


def gradient_strip(colors, n: int):
    """複数色を均等に並べたグラデーションの色リスト（長さ n）。"""
    cols = [_rgba(c) for c in colors]
    if len(cols) == 1:
        return [cols[0]] * n
    out = []
    for i in range(n):
        t = i / max(1, n - 1) * (len(cols) - 1)
        k = min(int(t), len(cols) - 2)
        out.append(lerp(cols[k], cols[k + 1], t - k))
    return out


class Card:
    def __init__(self, w: int, h: int, scale: float = 2, bg=(16, 17, 22)):
        self.w, self.h, self.s = w, h, scale
        self.im = Image.new("RGBA", (round(w * scale), round(h * scale)), _rgba(bg))

    # ---- 座標変換 ----
    def p(self, v: float) -> int:
        return round(v * self.s)

    def box(self, x, y, w, h):
        X, Y = self.p(x), self.p(y)
        return X, Y, max(1, self.p(x + w) - X), max(1, self.p(y + h) - Y)

    # ---- 合成 ----
    def _fill_mask(self, X, Y, mask: Image.Image, fill):
        fill = _rgba(fill)
        layer = Image.new("RGBA", mask.size, fill[:3] + (0,))
        alpha = mask if fill[3] == 255 else mask.point(lambda v, a=fill[3]: v * a // 255)
        layer.putalpha(alpha)
        self.im.alpha_composite(layer, (X, Y))

    def _fill_image(self, X, Y, img: Image.Image, mask: Image.Image, alpha: int = 255):
        img = img.convert("RGBA")
        a = ImageChops.multiply(img.getchannel("A"), mask)
        if alpha != 255:
            a = a.point(lambda v: v * alpha // 255)
        img.putalpha(a)
        self.im.alpha_composite(img, (X, Y))

    # ---- 図形 ----
    def rrect(self, x, y, w, h, r=8, fill=None, outline=None, width=1.0, gradient=None,
              gradient_dir="h", gradient_alpha=255):
        X, Y, W, H = self.box(x, y, w, h)
        R = min(self.p(r), W // 2, H // 2)
        mask = _rr_mask(W, H, R)
        if gradient:
            self._fill_image(X, Y, self.gradient_image(W, H, gradient, gradient_dir), mask, gradient_alpha)
        elif fill is not None:
            self._fill_mask(X, Y, mask, fill)
        if outline is not None:
            t = max(1, self.p(width))
            inner = Image.new("L", (W, H), 0)
            if W > 2 * t and H > 2 * t:
                inner.paste(_rr_mask(W - 2 * t, H - 2 * t, max(0, R - t)), (t, t))
            self._fill_mask(X, Y, ImageChops.subtract(mask, inner), outline)

    def paste_rrect(self, img: Image.Image, x, y, w, h, r=8):
        """カードと同じ大きさの画像 img から、角丸の範囲だけを同じ位置に貼る（すりガラスの下地用）。"""
        X, Y, W, H = self.box(x, y, w, h)
        R = min(self.p(r), W // 2, H // 2)
        self._fill_image(X, Y, img.crop((X, Y, X + W, Y + H)), _rr_mask(W, H, R))

    def shadow(self, x, y, w, h, r=8, blur=10, offset=(0, 4), color=(0, 0, 0, 110)):
        pad = self.p(blur * 2)
        X, Y, W, H = self.box(x + offset[0], y + offset[1], w, h)
        m = Image.new("L", (W + pad * 2, H + pad * 2), 0)
        m.paste(_rr_mask(W, H, min(self.p(r), W // 2, H // 2)), (pad, pad))
        m = m.filter(ImageFilter.GaussianBlur(self.p(blur) / 2))
        self._fill_mask(X - pad, Y - pad, m, color)

    def gradient_image(self, W, H, colors, direction="h") -> Image.Image:
        n = W if direction == "h" else H
        strip = gradient_strip(colors, n)
        line = Image.new("RGBA", (n, 1))
        line.putdata(strip)
        if direction == "h":
            return line.resize((W, H), Image.NEAREST)
        return line.rotate(-90, expand=True).resize((W, H), Image.NEAREST)

    def glow(self, cx, cy, radius, color, alpha=120):
        """ふわっとした円形の光（背景の雰囲気づくり用）。"""
        D = self.p(radius * 2)
        g = Image.radial_gradient("L").resize((D, D), Image.BILINEAR)
        g = ImageChops.invert(g).point(lambda v: int((v / 255) ** 2.2 * alpha))
        self._fill_mask(self.p(cx - radius), self.p(cy - radius), g, _rgba(color)[:3] + (255,))

    def line(self, x1, y1, x2, y2, fill, width=1.0):
        fill = _rgba(fill)
        layer = Image.new("RGBA", self.im.size, (0, 0, 0, 0))
        ImageDraw.Draw(layer).line((self.p(x1), self.p(y1), self.p(x2), self.p(y2)), fill=fill,
                                   width=max(1, self.p(width)))
        self.im.alpha_composite(layer)

    def ring(self, cx, cy, r, width, frac, color, track=(255, 255, 255, 25)):
        """円形ゲージ（frac = 0〜1）。color に色のリストを渡すとグラデーション。"""
        ss = 4
        D = self.p(r * 2)
        S = D * ss
        wpx = max(1, self.p(width)) * ss
        box = (0, 0, S - 1, S - 1)  # 線は外周から内側に向かって描かれる
        bt = Image.new("L", (S, S), 0)
        ImageDraw.Draw(bt).ellipse(box, outline=255, width=wpx)
        big = Image.new("L", (S, S), 0)
        frac = max(0.0, min(1.0, frac))
        if frac > 0:
            d = ImageDraw.Draw(big)
            d.arc(box, -90, -90 + 360 * frac, fill=255, width=wpx)
            rr = S / 2 - wpx / 2
            for ang in (-90, -90 + 360 * frac):  # 端を丸く
                a = math.radians(ang)
                px, py = S / 2 + rr * math.cos(a), S / 2 + rr * math.sin(a)
                d.ellipse((px - wpx / 2, py - wpx / 2, px + wpx / 2, py + wpx / 2), fill=255)
        X, Y = self.p(cx - r), self.p(cy - r)
        self._fill_mask(X, Y, bt.resize((D, D), Image.BOX), track)
        arc = big.resize((D, D), Image.BOX)
        if isinstance(color, list):
            self._fill_image(X, Y, self.gradient_image(D, D, color, "h"), arc)
        else:
            self._fill_mask(X, Y, arc, color)

    def segbar(self, x, y, w, h, parts, r=None, track=(255, 255, 255, 18)):
        """角丸の積み上げ横棒。parts = [(割合0〜1, 色), ...]"""
        X, Y, W, H = self.box(x, y, w, h)
        R = self.p(r if r is not None else h / 2)
        mask = _rr_mask(W, H, min(R, H // 2))
        self._fill_mask(X, Y, mask, track)
        img = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        d = ImageDraw.Draw(img)
        sx = 0.0
        for frac, col in parts:
            ex = sx + frac * W
            if round(ex) > round(sx):
                d.rectangle((round(sx), 0, round(ex) - 1, H), fill=_rgba(col))
            sx = ex
        self._fill_image(X, Y, img, mask)

    def paste(self, img: Image.Image, x, y, w=None, h=None, resample=Image.NEAREST, alpha=255):
        W = self.p(w) if w else round(img.width * self.s)
        H = self.p(h) if h else round(img.height * self.s)
        im = img.convert("RGBA").resize((W, H), resample) if (W, H) != img.size else img.convert("RGBA")
        if alpha != 255:
            a = im.getchannel("A").point(lambda v: v * alpha // 255)
            im.putalpha(a)
        self.im.alpha_composite(im, (self.p(x), self.p(y)))

    def paste_px(self, img: Image.Image, X: int, Y: int):
        """実ピクセル座標で貼る（スキン描画など、すでに実サイズで作った画像用）。"""
        self.im.alpha_composite(img.convert("RGBA"), (X, Y))

    # ---- Minecraft風の文字 ----
    def unit(self, u: float) -> int:
        return max(1, round(u * self.s))

    def mc(self, text, x, y, unit=2, anchor="l", default="f", shadow=True, shadow_color=None) -> float:
        return T.draw(self.im, x * self.s, y * self.s, text, self.unit(unit), default, anchor, shadow,
                      shadow_color) / self.s

    def mc_width(self, text, unit=2) -> float:
        return T.measure(text, self.unit(unit)) / self.s

    def mc_fit(self, text, max_w, units=(3, 2.5, 2, 1.5, 1)) -> float:
        for u in units:
            if self.mc_width(text, u) <= max_w:
                return u
        return units[-1]

    def mc_trunc(self, text, max_w, unit=2) -> str:
        """幅に収まらない文字列の末尾を削って「..」を付ける。"""
        if self.mc_width(text, unit) <= max_w:
            return text
        while text:
            if len(text) >= 2 and text[-2] == "§":
                text = text[:-2]
                continue
            text = text[:-1]
            if self.mc_width(text + "..", unit) <= max_w:
                return text + ".."
        return ".."

    # ---- 出力 ----
    def to_file(self, fmt: str = "PNG") -> "CardImage":
        fmt = fmt.upper()
        buf = CardImage()
        img = self.im.convert("RGB")
        if fmt == "WEBP":
            img.save(buf, "WEBP", quality=92, method=4)
            buf.ext = "webp"
        elif fmt in ("JPG", "JPEG"):
            img.save(buf, "JPEG", quality=92, subsampling=0)
            buf.ext = "jpg"
        else:
            if getattr(self, "transparent", False):
                img = self.im  # 背景を透明のまま保存（Discordの背景色になじむ）
            img.save(buf, "PNG", compress_level=6)
            buf.ext = "png"
        buf.seek(0)
        return buf


class CardImage(io.BytesIO):
    """描いたカード画像。ext に拡張子（png / jpg / webp）が入っています。"""
    ext = "png"


# ---------------------------------------------------------------- 背景
def cover(img: Image.Image, W: int, H: int) -> Image.Image:
    """画像を縦横比を保ったまま (W, H) を覆うように切り抜く。"""
    s = max(W / img.width, H / img.height)
    im = img.convert("RGB").resize((max(W, round(img.width * s)), max(H, round(img.height * s))), Image.LANCZOS)
    x, y = (im.width - W) // 2, (im.height - H) // 2
    return im.crop((x, y, x + W, y + H))


def photo_background(img: Image.Image, W: int, H: int, blur: float = 6, darken: float = 0.35) -> Image.Image:
    """スクリーンショットなどを背景用に（全体を覆うように切り抜き→ぼかし→少し暗く）。"""
    im = cover(img, W, H).filter(ImageFilter.GaussianBlur(blur))
    return Image.blend(im, Image.new("RGB", im.size, (0, 0, 0)), darken).convert("RGBA")
