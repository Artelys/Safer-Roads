import { resolve } from 'path';
import { existsSync } from 'fs';


import { i18n } from '@kbn/i18n';

import to_python from './server/routes/to_python';
import get_date_server from './server/routes/get_date_server';

export default function (kibana) {
  return new kibana.Plugin({
    require: ['elasticsearch'],
    name: 'data_management',
    uiExports: {
      app: {
        title: 'Gestion de données',
        description: 'Manage data in elasticsearch',
        main: 'plugins/data_management/app',
      },
      hacks: [
        'plugins/data_management/hack'
      ],
      styleSheetPaths: [resolve(__dirname, 'public/app.scss'), resolve(__dirname, 'public/app.css')].find(p => existsSync(p)),
    },

    config(Joi) {
      return Joi.object({
        enabled: Joi.boolean().default(true),
      }).default();
    },

    init(server, options) { // eslint-disable-line no-unused-vars
        const xpackMainPlugin = server.plugins.xpack_main;
        if (xpackMainPlugin) {
          const featureId = 'data_management';

          xpackMainPlugin.registerFeature({
            id: featureId,
            name: i18n.translate('dataManagement.featureRegistry.featureName', {
              defaultMessage: 'data_management',
            }),
            navLinkId: featureId,
            icon: 'questionInCircle',
            app: [featureId, 'kibana'],
            catalogue: [],
            privileges: {
              all: {
                api: [],
                savedObject: {
                  all: [],
                  read: [],
                },
                ui: ['show'],
              },
              read: {
                api: [],
                savedObject: {
                  all: [],
                  read: [],
                },
                ui: ['show'],
              },
            },
          });
        }

      // Add server routes and initialize the plugin here
     	get_date_server(server);
	to_python(server);
    }
  });
}
