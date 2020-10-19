import envSetter.auth as auth

class EnvConfig():
    def __init__(self, env_type, section):
        self.env_type = env_type
        self.section = section
        # print(self.env_type)
        """
        Args:
            - env_type: string type
                - 'pie': pie environment for testing
                - 'prd': production environment for implementation
        """
        if self.env_type == 'pie':
            self.auth_app = auth.Authenticator(env_name='pie', section=self.section)
            self.REF_BASE_URL = 'https://reference-entryapi-pie.weu.svc.origination.engineering.ase.dlgroup.com/'
            self.COMPANY_BASE_URL = 'https://company-entryapi-pie.weu.svc.origination.engineering.ase.dlgroup.com/v2/Company/'
            self.POST_URL = "https://dcm-entryapi-pie.weu.svc.origination.engineering.ase.dlgroup.com/v1/DCM/Deal"
            self.PUT_BASE_URL = 'https://dcm-entryapi-pie.weu.svc.origination.engineering.ase.dlgroup.com/v1/DCM/Deal/'
            self.GET_BASE_URL = 'https://dcm-entryapi-pie.weu.svc.origination.engineering.ase.dlgroup.com/v1/DCM/Deal/'
            self.PRECALC_URL = 'https://dcm-entryapi-pie.weu.svc.origination.engineering.ase.dlgroup.com/v1/DCM/Deal/Precalculate'
        elif self.env_type == 'prd':
            self.auth_app = auth.Authenticator(env_name='prd', section=self.section)
            self.REF_BASE_URL = 'https://reference-entryapi-prd.weu.svc.origination.colocation.ase.dlgroup.com/'
            self.COMPANY_BASE_URL = 'https://company-entryapi-prd.weu.svc.origination.colocation.ase.dlgroup.com/v2/Company/'
            self.POST_URL = 'https://dcm-entryapi-prd.weu.svc.origination.colocation.ase.dlgroup.com/v1/DCM/Deal'
            self.PUT_BASE_URL = 'https://dcm-entryapi-prd.weu.svc.origination.colocation.ase.dlgroup.com/v1/DCM/Deal/'
            self.GET_BASE_URL = 'https://dcm-entryapi-prd.weu.svc.origination.colocation.ase.dlgroup.com/v1/DCM/Deal/'
            self.PRECALC_URL = 'https://dcm-entryapi-prd.weu.svc.origination.colocation.ase.dlgroup.com/v1/DCM/Deal/Precalculate'
        else:
            raise ValueError('Wrong environment type')

    def get_env_info(self):
        return (self.section, self.env_type)