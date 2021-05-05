export default function (server) {
	server.route({
    path: '/api/risk_prediction/to_python',
    method: 'POST',
    handler: async function(req, reply) {
		const request = require('request');
		var res = await new Promise((resolve, reject) => {
        	request('http://flask:5000/'+req.payload.path_python, 
        		{method: 'POST', "headers":{"Content-Type": "application/json"}, body: JSON.stringify(req.payload)},
        		(error, response, body) => {

		            if (error) reject(error);
		            if(typeof(response)=="undefined"){   
					  reject(error);   
					}
		            else if (response.statusCode != 200) {

		                reject('Invalid status code <' + response.statusCode + '>');
		            }
				console.lo("not rejected");
		            res = response.body;
		            resolve(res);
	            
	        });
	    });
		return res;
    	}
	})
}
