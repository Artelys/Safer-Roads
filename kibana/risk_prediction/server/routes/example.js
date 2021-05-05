export default function (server) {

  server.route({
    path: '/api/risk_prediction/example',
    method: 'GET',
    handler() {
      return { time: (new Date()).toISOString() };
    }
  });

}
