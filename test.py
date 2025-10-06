from satctl.config import get_settings

if __name__ == "__main__":
    cfg = get_settings()
    print(id(cfg))
    cfg2 = get_settings()
    print(id(cfg))
