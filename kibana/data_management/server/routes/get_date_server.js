export default function (server) {
	server.route({
    path: '/api/data_management/get_date_server',
    method: 'POST',
    handler: async function (req, reply) {
		
		var today = new Date();
		var year = today.getFullYear();
		var month = today.getMonth()+1;
		var day = today.getDate();
		var hour = today.getHours();
		var minute = today.getMinutes();
		var second = today.getSeconds();
		if (month < 10){
			month = "0"+month;
		}
		if (day < 10){
			day = "0"+day;
		}
		if (minute < 10){
			minute = "0"+minute;
		}
		if (hour < 10){
			hour = "0"+hour;
		}
		if (second < 10){
			second = "0"+second;
		}
		var date = year + "." + month + "." + day + "." + hour + "." + minute + "." + second;
		return date;
	}
	})

}