import yaml
import subprocess

with open("/var/lib/hbn/etc/nvue.d/startup.yaml") as f:
    y = yaml.safe_load(f)
    if y is not None:
        if len(y) >= 2:
            s=y[1]
            if isinstance(s, dict):
                if 'set' in s:
                    if 'system' in s['set']:
                        if 'api' in s['set']['system']:
                            if 'listening-address' in s['set']['system']['api']:
                                if '0.0.0.0' in s['set']['system']['api']['listening-address']:
                                     pass
                                else:
                                     s['set']['system']['api']['listening-address']['0.0.0.0'] = {}
                            else:
                                s['set']['system']['api'].update({'listening-address': {'0.0.0.0': {}}})
                        else:
                            s['set']['system']['api'] = {'listening-address': {'0.0.0.0': {}}}
                    else:
                        s['set']['system'] = {'api': {'listening-address': {'0.0.0.0': {}}}}
                y[1] = s

                with open("/var/lib/hbn/etc/nvue.d/startup.yaml", mode='w') as nf:
                    yaml.dump(y, nf)
                    nf.close()
            else:
                subprocess.Popen('cp etc/nvue.d/enable-rest.yaml /var/lib/hbn/etc/nvue.d/startup.yaml', shell=True)
        f.close()
    else:
        subprocess.Popen('cp etc/nvue.d/enable-rest.yaml /var/lib/hbn/etc/nvue.d/startup.yaml', shell=True)
        f.close()
