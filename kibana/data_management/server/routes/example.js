export default function (server) {

  server.route({
    path: '/api/data_management/example',
    method: 'GET',
    handler() {
      return { time: (new Date()).toISOString() };
    }
  });

}
