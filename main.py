"""
main.py — Market Data Fetcher · Interface Kivy para Android
Fluxo: HomeScreen → SymbolScreen → ContractScreen → PresetScreen
       → CustomScreen (opcional) → FetchScreen
"""

import os
import threading

from kivy.app import App
from kivy.clock import Clock
from kivy.core.window import Window
from kivy.metrics import dp, sp
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.button import Button
from kivy.uix.gridlayout import GridLayout
from kivy.uix.label import Label
from kivy.uix.screenmanager import Screen, ScreenManager, SlideTransition
from kivy.uix.scrollview import ScrollView
from kivy.uix.textinput import TextInput
from kivy.uix.widget import Widget

from fetcher_core import (
    COINALYZE_MAP, TF_PRESETS, PROTOCOL_TFS,
    build_tf_config, run_extraction
)

# ─── PALETA ──────────────────────────────────────────────────────────────────
BG       = (0.08, 0.08, 0.10, 1)
SURFACE  = (0.13, 0.13, 0.17, 1)
ACCENT   = (0.20, 0.55, 0.95, 1)
ACCENT2  = (0.15, 0.42, 0.72, 1)
SUCCESS  = (0.18, 0.72, 0.45, 1)
DANGER   = (0.85, 0.25, 0.25, 1)
TEXT     = (0.92, 0.92, 0.95, 1)
MUTED    = (0.55, 0.55, 0.62, 1)
DISABLED = (0.30, 0.30, 0.35, 1)

# ─── HELPERS DE WIDGET ───────────────────────────────────────────────────────
def make_btn(text, on_press, color=ACCENT, height=dp(52), font_size=sp(15)):
    btn = Button(
        text=text, size_hint=(1, None), height=height,
        background_normal="", background_color=color,
        color=TEXT, font_size=font_size, bold=True
    )
    btn.bind(on_press=on_press)
    return btn

def make_label(text, font_size=sp(14), color=TEXT, bold=False,
               size_hint_y=None, height=None, halign="left"):
    lbl = Label(
        text=text, font_size=font_size, color=color, bold=bold,
        size_hint_y=size_hint_y, halign=halign, markup=True
    )
    if height: lbl.height = height
    lbl.bind(size=lambda inst, v: setattr(inst, "text_size", (v[0], None)))
    return lbl

def make_input(hint="", multiline=False, height=dp(44)):
    ti = TextInput(
        hint_text=hint, multiline=multiline,
        size_hint=(1, None), height=height,
        background_color=SURFACE, foreground_color=TEXT,
        hint_text_color=list(MUTED), cursor_color=list(ACCENT),
        font_size=sp(15), padding=[dp(12), dp(10)]
    )
    return ti

def spacer(h=dp(12)):
    w = Widget(size_hint=(1, None), height=h)
    return w

def make_screen_layout():
    """BoxLayout padrão para o conteúdo de cada tela."""
    layout = BoxLayout(
        orientation="vertical",
        padding=[dp(20), dp(24), dp(20), dp(20)],
        spacing=dp(12)
    )
    return layout

# ─── ESTADO GLOBAL DO APP ────────────────────────────────────────────────────
class AppState:
    asset_type   = "equity"
    symbol       = ""
    contract     = "spot"
    tf_config    = None
    output_dir   = ""
    saved_files  = []
    custom_mins  = {"1w": 120, "1d": 170, "4h": 300, "1h": 250}

state = AppState()

# ─── TELAS ───────────────────────────────────────────────────────────────────

class HomeScreen(Screen):
    def __init__(self, **kw):
        super().__init__(**kw)
        layout = make_screen_layout()

        layout.add_widget(spacer(dp(30)))
        layout.add_widget(make_label(
            "[b]Market Data Fetcher[/b]", font_size=sp(22),
            color=ACCENT, bold=True, halign="center",
            size_hint_y=None, height=dp(40)
        ))
        layout.add_widget(make_label(
            "Protocolo v7.0", font_size=sp(13), color=MUTED,
            halign="center", size_hint_y=None, height=dp(24)
        ))
        layout.add_widget(spacer(dp(30)))
        layout.add_widget(make_label(
            "Selecione a classe do ativo:", font_size=sp(15),
            color=MUTED, size_hint_y=None, height=dp(24)
        ))
        layout.add_widget(spacer(dp(8)))
        layout.add_widget(make_btn(
            "📈  Equity  (ações · ETF · índices)",
            on_press=lambda x: self.go("equity"), height=dp(60)
        ))
        layout.add_widget(make_btn(
            "₿  Crypto  (Binance · Bybit)",
            on_press=lambda x: self.go("crypto"),
            color=SUCCESS, height=dp(60)
        ))
        layout.add_widget(Widget())
        self.add_widget(layout)

    def go(self, asset_type):
        state.asset_type = asset_type
        self.manager.transition = SlideTransition(direction="left")
        self.manager.current = "symbol"


class SymbolScreen(Screen):
    def __init__(self, **kw):
        super().__init__(**kw)
        layout = make_screen_layout()
        layout.add_widget(make_label("[b]Símbolo[/b]", font_size=sp(18),
                                     size_hint_y=None, height=dp(32)))
        layout.add_widget(make_label(
            "Ex: SPY · AAPL · TSLA  ou  BTCUSDT · ETHUSDT",
            font_size=sp(13), color=MUTED, size_hint_y=None, height=dp(24)
        ))
        layout.add_widget(spacer(dp(8)))
        self.ti = make_input(hint="Digite o símbolo...")
        layout.add_widget(self.ti)
        layout.add_widget(spacer())
        layout.add_widget(make_btn("Continuar →", on_press=self.go))
        layout.add_widget(make_btn("← Voltar", on_press=self.back,
                                   color=SURFACE, height=dp(44)))
        layout.add_widget(Widget())
        self.add_widget(layout)

    def on_pre_enter(self):
        self.ti.text = state.symbol

    def go(self, *_):
        sym = self.ti.text.strip().upper()
        if not sym:
            self.ti.hint_text = "⚠️ Símbolo obrigatório"; return
        state.symbol = sym
        self.manager.transition = SlideTransition(direction="left")
        if state.asset_type == "crypto":
            self.manager.current = "contract"
        else:
            state.contract = "spot"
            self.manager.current = "preset"

    def back(self, *_):
        self.manager.transition = SlideTransition(direction="right")
        self.manager.current = "home"


class ContractScreen(Screen):
    """Apenas para Crypto."""
    def __init__(self, **kw):
        super().__init__(**kw)
        layout = make_screen_layout()
        layout.add_widget(make_label("[b]Tipo de Contrato[/b]", font_size=sp(18),
                                     size_hint_y=None, height=dp(32)))
        layout.add_widget(spacer(dp(16)))
        layout.add_widget(make_btn(
            "🔵  Spot\nCVD via Binance taker buy",
            on_press=lambda x: self.go("spot"), height=dp(70)
        ))
        layout.add_widget(make_btn(
            "🟣  Perpétuo\nCVD + Funding + LSR via Coinalyze",
            on_press=lambda x: self.go("perp"),
            color=SUCCESS, height=dp(70)
        ))
        layout.add_widget(spacer())
        layout.add_widget(make_btn("← Voltar", on_press=self.back,
                                   color=SURFACE, height=dp(44)))
        layout.add_widget(Widget())
        self.add_widget(layout)

    def go(self, contract):
        state.contract = contract
        if contract == "perp":
            sym = state.symbol
            coin_sym = COINALYZE_MAP.get((sym, "perp"), "")
            if not coin_sym:
                self.manager.current = "coinalyze"
                return
        self.manager.transition = SlideTransition(direction="left")
        self.manager.current = "preset"

    def back(self, *_):
        self.manager.transition = SlideTransition(direction="right")
        self.manager.current = "symbol"


class CoinalyzeScreen(Screen):
    """Tela extra para símbolo Coinalyze não mapeado."""
    def __init__(self, **kw):
        super().__init__(**kw)
        layout = make_screen_layout()
        layout.add_widget(make_label("[b]Símbolo Coinalyze[/b]", font_size=sp(18),
                                     size_hint_y=None, height=dp(32)))
        self.hint_lbl = make_label("", font_size=sp(13), color=MUTED,
                                    size_hint_y=None, height=dp(40))
        layout.add_widget(self.hint_lbl)
        layout.add_widget(spacer(dp(8)))
        self.ti = make_input(hint="ex: BTCUSDT_PERP.A")
        layout.add_widget(self.ti)
        layout.add_widget(spacer())
        layout.add_widget(make_btn("Continuar →", on_press=self.go))
        layout.add_widget(make_btn("Pular (sem CVD)", on_press=self.skip,
                                   color=SURFACE, height=dp(44)))
        layout.add_widget(make_btn("← Voltar", on_press=self.back,
                                   color=SURFACE, height=dp(44)))
        layout.add_widget(Widget())
        self.add_widget(layout)

    def on_pre_enter(self):
        self.hint_lbl.text = (f"Símbolo não mapeado para [b]{state.symbol}[/b].\n"
                              f"Sugerido: {state.symbol}_PERP.A")

    def go(self, *_):
        val = self.ti.text.strip()
        if not val: return
        COINALYZE_MAP[(state.symbol, "perp")] = val
        self.manager.transition = SlideTransition(direction="left")
        self.manager.current = "preset"

    def skip(self, *_):
        self.manager.transition = SlideTransition(direction="left")
        self.manager.current = "preset"

    def back(self, *_):
        self.manager.transition = SlideTransition(direction="right")
        self.manager.current = "contract"


class PresetScreen(Screen):
    def __init__(self, **kw):
        super().__init__(**kw)
        self.layout = make_screen_layout()
        self.layout.add_widget(make_label("[b]Preset de Candles[/b]", font_size=sp(18),
                                           size_hint_y=None, height=dp(32)))
        self.subtitle = make_label("", font_size=sp(13), color=MUTED,
                                    size_hint_y=None, height=dp(20))
        self.layout.add_widget(self.subtitle)
        self.layout.add_widget(spacer(dp(12)))
        self.btn_p1 = make_btn("", on_press=lambda x: self.pick("p1"), height=dp(60))
        self.btn_p2 = make_btn("", on_press=lambda x: self.pick("p2"),
                               color=ACCENT2, height=dp(60))
        self.btn_custom = make_btn("⚙️  Customizado — definir por TF",
                                   on_press=lambda x: self.pick("custom"),
                                   color=SURFACE, height=dp(60))
        self.layout.add_widget(self.btn_p1)
        self.layout.add_widget(self.btn_p2)
        self.layout.add_widget(self.btn_custom)
        self.layout.add_widget(spacer())
        self.layout.add_widget(make_btn("← Voltar", on_press=self.back,
                                        color=SURFACE, height=dp(44)))
        self.layout.add_widget(Widget())
        self.add_widget(self.layout)

    def on_pre_enter(self):
        at = state.asset_type
        if at == "equity":
            self.subtitle.text = "Equity — mercado ~6.5h/dia"
            p1k, p2k = "equity_otimizado", "equity_original"
        else:
            self.subtitle.text = "Crypto — mercado 24/7"
            p1k, p2k = "crypto_otimizado", "crypto_original"
        self._p1k, self._p2k = p1k, p2k
        p1 = TF_PRESETS[p1k]; p2 = TF_PRESETS[p2k]
        self.btn_p1.text = f"⚡ Otimizado\n1W:{p1['1w']} · 1D:{p1['1d']} · 4H:{p1['4h']} · 1H:{p1['1h']}"
        self.btn_p2.text = f"📚 Original\n1W:{p2['1w']} · 1D:{p2['1d']} · 4H:{p2['4h']} · 1H:{p2['1h']}"

    def pick(self, choice):
        if choice == "p1":
            state.tf_config = build_tf_config(self._p1k)
            self._go_fetch()
        elif choice == "p2":
            state.tf_config = build_tf_config(self._p2k)
            self._go_fetch()
        else:
            at = state.asset_type
            defaults = TF_PRESETS["equity_otimizado" if at=="equity" else "crypto_otimizado"]
            state.custom_mins = dict(defaults)
            self.manager.transition = SlideTransition(direction="left")
            self.manager.current = "custom"

    def _go_fetch(self):
        self.manager.transition = SlideTransition(direction="left")
        self.manager.current = "fetch"

    def back(self, *_):
        self.manager.transition = SlideTransition(direction="right")
        if state.asset_type == "crypto":
            self.manager.current = "contract"
        else:
            self.manager.current = "symbol"


class CustomScreen(Screen):
    def __init__(self, **kw):
        super().__init__(**kw)
        self.layout = make_screen_layout()
        self.layout.add_widget(make_label("[b]Candles Customizados[/b]", font_size=sp(18),
                                           size_hint_y=None, height=dp(32)))
        self.layout.add_widget(make_label(
            "Enter = valor sugerido", font_size=sp(12), color=MUTED,
            size_hint_y=None, height=dp(20)
        ))
        self.layout.add_widget(spacer(dp(8)))
        self.inputs = {}
        grid = GridLayout(cols=2, spacing=dp(12), size_hint=(1, None),
                          height=dp(4 * 70))
        for tf_key, label in [("1w","1W  (semanas)"), ("1d","1D  (dias)"),
                               ("4h","4H  (4 horas)"), ("1h","1H  (1 hora)")]:
            grid.add_widget(make_label(label, font_size=sp(14),
                                       size_hint_y=None, height=dp(44),
                                       color=MUTED))
            ti = make_input(hint="...")
            ti.input_filter = "int"
            self.inputs[tf_key] = ti
            grid.add_widget(ti)
        self.layout.add_widget(grid)
        self.layout.add_widget(spacer())
        self.layout.add_widget(make_btn("✅  Confirmar e Extrair", on_press=self.go))
        self.layout.add_widget(make_btn("← Voltar", on_press=self.back,
                                        color=SURFACE, height=dp(44)))
        self.layout.add_widget(Widget())
        self.add_widget(self.layout)

    def on_pre_enter(self):
        for tf_key, ti in self.inputs.items():
            ti.text = str(state.custom_mins.get(tf_key, ""))

    def go(self, *_):
        custom = {}
        for tf_key, ti in self.inputs.items():
            val = ti.text.strip()
            custom[tf_key] = int(val) if val.isdigit() and int(val) > 0 else state.custom_mins[tf_key]
        state.custom_mins = custom
        state.tf_config = build_tf_config(None, custom_mins=custom)
        self.manager.transition = SlideTransition(direction="left")
        self.manager.current = "fetch"

    def back(self, *_):
        self.manager.transition = SlideTransition(direction="right")
        self.manager.current = "preset"


class FetchScreen(Screen):
    def __init__(self, **kw):
        super().__init__(**kw)
        self._lock = threading.Lock()

        outer = BoxLayout(orientation="vertical",
                          padding=[dp(16), dp(20), dp(16), dp(16)],
                          spacing=dp(10))

        # Header
        self.title_lbl = make_label("[b]Extraindo...[/b]", font_size=sp(17),
                                     size_hint_y=None, height=dp(30))
        self.sub_lbl   = make_label("", font_size=sp(12), color=MUTED,
                                     size_hint_y=None, height=dp(20))
        outer.add_widget(self.title_lbl)
        outer.add_widget(self.sub_lbl)
        outer.add_widget(spacer(dp(6)))

        # Log area
        scroll = ScrollView(size_hint=(1, 1))
        self.log_box = BoxLayout(orientation="vertical", size_hint_y=None,
                                  spacing=dp(2), padding=[dp(4), dp(4)])
        self.log_box.bind(minimum_height=self.log_box.setter("height"))
        scroll.add_widget(self.log_box)
        outer.add_widget(scroll)

        outer.add_widget(spacer(dp(6)))

        # Botões de ação
        self.btn_share = make_btn("📤  Compartilhar Arquivos",
                                   on_press=self.share_files,
                                   color=SUCCESS)
        self.btn_share.opacity = 0
        self.btn_share.disabled = True

        self.btn_new = make_btn("🔄  Nova Extração",
                                 on_press=self.new_extraction,
                                 color=ACCENT2, height=dp(44))
        self.btn_new.opacity = 0
        self.btn_new.disabled = True

        outer.add_widget(self.btn_share)
        outer.add_widget(self.btn_new)
        self.add_widget(outer)

    def on_pre_enter(self):
        # Limpar estado anterior
        self.log_box.clear_widgets()
        self.btn_share.opacity = 0; self.btn_share.disabled = True
        self.btn_new.opacity   = 0; self.btn_new.disabled   = True

        sym = state.symbol; at = state.asset_type; ct = state.contract
        cfg = state.tf_config
        mins = {tf: cfg[tf]["min"] for tf in PROTOCOL_TFS}

        self.title_lbl.text = f"[b]{sym} {ct.upper()} — {at.upper()}[/b]"
        self.sub_lbl.text   = (f"1W:{mins['1w']} · 1D:{mins['1d']} "
                               f"· 4H:{mins['4h']} · 1H:{mins['1h']}")

        # Resolver diretório de saída
        try:
            from android.storage import primary_external_storage_path
            base = primary_external_storage_path()
        except ImportError:
            base = os.path.expanduser("~")
        state.output_dir = os.path.join(base, "MarketDataFetcher")
        os.makedirs(state.output_dir, exist_ok=True)

        # Rodar em thread para não bloquear a UI
        t = threading.Thread(target=self._run, daemon=True)
        t.start()

    def _run(self):
        try:
            files = run_extraction(
                symbol      = state.symbol,
                contract    = state.contract,
                asset_type  = state.asset_type,
                tf_config   = state.tf_config,
                output_dir  = state.output_dir,
                log_cb      = self._log_cb,
            )
            state.saved_files = files
            Clock.schedule_once(lambda dt: self._on_done(success=True), 0)
        except Exception as e:
            self._log_cb(f"\n❌ ERRO: {e}")
            Clock.schedule_once(lambda dt: self._on_done(success=False), 0)

    def _log_cb(self, msg):
        """Chamado da thread de extração — agenda atualização na thread principal."""
        Clock.schedule_once(lambda dt: self._add_log(msg), 0)

    def _add_log(self, msg):
        lbl = Label(
            text=msg, font_size=sp(12), color=TEXT,
            size_hint_y=None, halign="left",
            text_size=(Window.width - dp(60), None)
        )
        lbl.bind(texture_size=lambda inst, v: setattr(inst, "height", v[1]+dp(4)))
        self.log_box.add_widget(lbl)

    def _on_done(self, success):
        if success:
            self._add_log("\n✅  Extração concluída!")
            self.btn_share.opacity = 1; self.btn_share.disabled = False
        else:
            self._add_log("Verifique sua conexão e tente novamente.")
        self.btn_new.opacity = 1; self.btn_new.disabled = False

    def share_files(self, *_):
        if not state.saved_files: return
        try:
            from android.permissions import request_permissions, Permission
            from jnius import autoclass
            request_permissions([Permission.READ_EXTERNAL_STORAGE,
                                  Permission.WRITE_EXTERNAL_STORAGE])
            Intent     = autoclass("android.content.Intent")
            Uri        = autoclass("android.net.Uri")
            File       = autoclass("java.io.File")
            ArrayList  = autoclass("java.util.ArrayList")
            PythonActivity = autoclass("org.kivy.android.PythonActivity")
            context    = PythonActivity.mActivity

            intent = Intent(Intent.ACTION_SEND_MULTIPLE)
            intent.setType("*/*")
            uris = ArrayList()
            for fp in state.saved_files:
                uri = Uri.fromFile(File(fp))
                uris.add(uri)
            intent.putParcelableArrayListExtra(Intent.EXTRA_STREAM, uris)
            intent.addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
            context.startActivity(Intent.createChooser(intent, "Compartilhar arquivos"))
        except Exception as e:
            self._add_log(f"Compartilhamento: {e}\nArquivos em: {state.output_dir}")

    def new_extraction(self, *_):
        self.manager.transition = SlideTransition(direction="right")
        self.manager.current = "home"


# ─── APP ─────────────────────────────────────────────────────────────────────
class MarketDataApp(App):
    def build(self):
        Window.clearcolor = BG

        sm = ScreenManager()
        sm.add_widget(HomeScreen(name="home"))
        sm.add_widget(SymbolScreen(name="symbol"))
        sm.add_widget(ContractScreen(name="contract"))
        sm.add_widget(CoinalyzeScreen(name="coinalyze"))
        sm.add_widget(PresetScreen(name="preset"))
        sm.add_widget(CustomScreen(name="custom"))
        sm.add_widget(FetchScreen(name="fetch"))
        return sm

    def get_application_name(self):
        return "Market Data Fetcher"


if __name__ == "__main__":
    MarketDataApp().run()
