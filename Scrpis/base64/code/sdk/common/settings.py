from .utils.osenv import OSEnv

WEB_DOMAIN = OSEnv.str("WEB_DOMAIN", "", description="主域名")
