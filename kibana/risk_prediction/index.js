import { resolve } from 'path';
import { existsSync } from 'fs';


import { i18n } from '@kbn/i18n';

import exampleRoute from './server/routes/example';

export default function (kibana) {
  return new kibana.Plugin({
    require: ['elasticsearch'],
    name: 'risk_prediction',
    uiExports: {
      app: {
        title: 'Prédiction',
        description: 'Prediction of risk',
        main: 'plugins/risk_prediction/app',
      },
      hacks: [
        'plugins/risk_prediction/hack'
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
          const featureId = 'risk_prediction';

          xpackMainPlugin.registerFeature({
            id: featureId,
            name: i18n.translate('riskPrediction.featureRegistry.featureName', {
              defaultMessage: 'risk_prediction',
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
      exampleRoute(server);
    }
  });
}
