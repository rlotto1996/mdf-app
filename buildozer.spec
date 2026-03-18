[app]
title           = Market Data Fetcher
package.name    = marketdatafetcher
package.domain  = com.analise.ativos
source.dir      = .
source.include_exts = py,json
version         = 1.0

# ─── DEPENDÊNCIAS ────────────────────────────────────────────────────────────
# pandas e numpy têm receitas nativas no python-for-android
requirements = python3,kivy==2.3.0,pandas,numpy,requests,plyer,android,pyjnius

# ─── ORIENTAÇÃO E TELA ───────────────────────────────────────────────────────
orientation     = portrait
fullscreen      = 0

# ─── ANDROID ─────────────────────────────────────────────────────────────────
android.permissions = INTERNET, WRITE_EXTERNAL_STORAGE, READ_EXTERNAL_STORAGE
android.api         = 33
android.minapi      = 21
android.ndk         = 25b
android.sdk         = 33
android.accept_sdk_license = True

# Evita crash em dispositivos sem GPU dedicada
android.archs = arm64-v8a, armeabi-v7a

# ─── ÍCONE E SPLASH (opcional — substituir pelos seus) ───────────────────────
# icon.filename     = %(source.dir)s/icon.png
# presplash.filename = %(source.dir)s/presplash.png

# ─── BUILDOZER ───────────────────────────────────────────────────────────────
[buildozer]
log_level = 2
warn_on_root = 1
