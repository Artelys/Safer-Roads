import React from 'react';
import {
  EuiPage,
  EuiPageHeader,
  EuiTitle,
  EuiPageBody,
  EuiPageContent,
  EuiPageContentHeader,
  EuiPageContentBody,
  EuiText,
  EuiButton,
  EuiFieldText,
  EuiSelect,
  EuiBasicTable,
  EuiSpacer,
  EuiSwitch,
  EuiCode,
  EuiHealth,
  EuiFlexItem,
  EuiFlexGroup,
  EuiFlexGrid
} from '@elastic/eui';

import { FormattedMessage, Text } from '@kbn/i18n/react';

import { IndexesTable } from "./indexes_table.js";

export class Main extends React.Component {
  constructor(props) {
    super(props);
    var options = [
                  { value: 'contexte', text: 'Contexte' },
                  { value: 'positions', text: 'Positions' },
                  { value: 'zones', text: 'Zones' },
                  { value: 'infos_routes', text: 'Informations sur les routes' },
                  { value: 'accidents_materiels', text: 'Accidents matériels' },
                  { value: 'accidents_corporels', text: 'Accidents corporels' },
                  { value: 'voirie_de_reference', text: 'Voirie de référence' }
                ]
    this.state = {
          "options":  options,
          "index_name" : "",
          "path_name" : "",
          "data" : "",
          "files" : "",
          "size" : 1024*256,
          "uploaded" : 0,
          "error": " ",
          "error_training": " ",
          "index":"",
          "name":"",
          "indexes":[],
          "ready":false,
          field_name:options[0].value,
          httpClient:this.props.httpClient
        };
    this.get_index();
    this.get_training_informations()
  }

  componentDidMount() {
  }
  async get_index(){
    // get index in elasticsearch
    var indexes = await this.state.httpClient.post('../api/data_management/to_python', 
          { 
            "path_python":"get_index",
          },
          ["Content-Type: application/x-ndjson"]
        );

    var indices = [];
    indexes = indexes.data;
    console.log(indexes);    
    for (var i in indexes){
      indices.push(indexes[i]);
      }
    this.setState({indexes:indices})
    

  }
  async handle_click(){
    var date = await this.state.httpClient.post('../api/data_management/get_date_server', 
        { 
        },
        ["Content-Type: application/json"]
      ).then((resp) => {
        return resp.data;
      },
        function(error){
      });

    // path, name, type
    this.state.httpClient.post('../api/data_management/to_python', 
          { 
            "type":this.state.field_name,
            "path":this.state.path_name,
            "name":this.state.index_name,
            "path_python":"send",
            "date":date
          },
          ["Content-Type: application/x-ndjson"]
        ).then((resp) => {
                },
          function(error){
    });
    this.state.correct_date = date;
    this.state.name = this.state.field_name+"-"+this.state.correct_date+"-"+this.state.index_name
    this.get_information();

  }
  handle_harmonisation(name){
      console.log("HARMONISATION");
      // name
      this.state.httpClient.post('../api/data_management/to_python', 
          { 
            "name":name,
            "path_python":"harmonise",
          },
          ["Content-Type: application/x-ndjson"]
        ).then((resp) => {
                console.log("END_HARMONISATION")
                },
          function(error){
      });
      //this.get_harmonisate_information(name);         
      
  }
  handle_training(){
      this.state.httpClient.post('../api/data_management/to_python', 
          { 
            "path_python":"training",
          },
          ["Content-Type: application/x-ndjson"]
        ).then((resp) => {
                },
          function(error){
      });
      //this.get_training_informations()
  }
  async get_information(){
      var main = this;
      // get information of data send to elasticsearch in the index informations_send
      var value = await this.state.httpClient.post('../api/data_management/to_python', 
          { 
            "type":"send",
            "name":this.state.name,
            "path_python":"get_error_send",
          },
          ["Content-Type: application/x-ndjson"]
        ).then((resp) => {
            return resp.data;
          },
          function(error){
    });
      if (value == ""){
        setTimeout(function() {
                                  main.get_information();
                              }, 60000);
      }
      else{
        this.setState({"error":value["error"]});
        this.get_index();
      }
  }

  async get_harmonisate_information(name){
      // get information of data send to elasticsearch in the index informations_send
      var value = await this.state.httpClient.post('../api/data_management/to_python', 
          { 
            "type":"harmonise",
            "name":name,
            "path_python":"get_error_send",
          },
          ["Content-Type: application/x-ndjson"]
        ).then((resp) => {
            return resp.data;
          },
          function(error){
    });
      if (value == ""){
        setTimeout(this.get_information.bind(this), 60000);
      }
      else{
        this.setState({"error":value["error"]});
        this.get_index();
      }
  }

  async get_training_informations(){
      var main = this;
      // get information of data send to elasticsearch in the index informations_send
      var values = await this.state.httpClient.post('../api/data_management/to_python', 
          { 
            "path_python":"get_infos_training",
          },
          ["Content-Type: application/x-ndjson"]
        ).then((resp) => {
            return resp.data;
          },
          function(error){
    });
        for (var i in values){
          if (i >= 3){
            break;
          }
          if (values[i]["Name"] == "Corporel"){
            this.setState({"corporel":values[i]});
          }
          if (values[i]["Name"] == "Materiel"){
            this.setState({"materiel":values[i]});
          }
          if (values[i]["Name"] == "Train" || values[i]["Name"] == "Training"){
            this.setState({"training":values[i]});
          }
        }
      if (values.length >= 2) {
		this.setState({"error_training":values[3]});
	  }
      this.setState({"ready":true});
  }

  on_change_path(e) {
    this.setState({"path_name": e.target.value});
  }
  on_change_field(e) {
    this.setState({"field_name": e.target.value});
  }
  on_change_name(e) {
    this.setState({"index_name": e.target.value});
  }


  render() {
    if (!this.state.ready){ 
      return null; 
    }
	var date_training = "NA";
	var error_training = "NA";
    var message_training = "";
	
	var corporel_precision = "NA";
	var materiel_precision = "NA";
	var corporel_recall = "NA";
	var materiel_recall = "NA";
	var corporel_giny = "NA";
	var materiel_giny = "NA";
	
	var corporel_confusion_0_0 = "NA";
	var corporel_confusion_1_0 = "NA";
	var corporel_confusion_0_1 = "NA";
	var corporel_confusion_1_1 = "NA";
	
	var materiel_confusion_0_0 = "NA";
	var materiel_confusion_1_0 = "NA";
	var materiel_confusion_0_1 = "NA";
	var materiel_confusion_1_1 = "NA";

	if (this.state.training) {
		date_training = this.state.training.Date;
		error_training = "Dernière information : " + this.state.error_training;
		if (this.state.training.Name == "Training"){
			message_training = "En cours";
		}
		else{
			message_training = "Prêt";
		}
	}
	if (this.state.corporel) {
		corporel_precision = this.state.corporel.Precision.toFixed(4);
		corporel_recall = this.state.corporel.Recall.toFixed(4);
		corporel_giny = this.state.corporel.Giny.toFixed(4);
		corporel_confusion_0_0 = this.state.corporel.Confusion[0][0];
		corporel_confusion_1_0 = this.state.corporel.Confusion[1][0];
		corporel_confusion_0_1 = this.state.corporel.Confusion[0][1];
		corporel_confusion_1_1 = this.state.corporel.Confusion[1][0];
	}
	if (this.state.materiel) {
		materiel_precision = this.state.materiel.Precision.toFixed(4);
		materiel_recall = this.state.materiel.Recall.toFixed(4);
		materiel_giny = this.state.materiel.Giny.toFixed(4);
		materiel_confusion_0_0 = this.state.materiel.Confusion[0][0];
		materiel_confusion_1_0 = this.state.materiel.Confusion[1][0];
		materiel_confusion_0_1 = this.state.materiel.Confusion[0][1];
		materiel_confusion_1_1 = this.state.materiel.Confusion[1][0];
	}
	

    var style_table = {
        "borderCollapse": "collapse",
        "width": "200px",
        "height": "100px",
        "tableLayout": "fixed"
    };

    const { title } = this.props;
    var items = this.state.indexes;
    return (
      <EuiPage>
        <EuiPageBody>
          <EuiPageHeader>
          </EuiPageHeader>
          <EuiPageContent>
            <EuiPageContentHeader>
            </EuiPageContentHeader>
            <EuiPageContentBody>
            <EuiText>
            <h1>Import des données</h1>
            <p>Envoyer les données dans le serveur via le répertoire monté /tmp/data sur le serveur 
            Ensuite, remplissez le formulaire ci-dessous<br/>
            Le supplément de nom pour l'index ne doit pas contenir de majuscule ni le terme "-". Ce nom vous permettra de différencier les sources de données.
            </p>

            </EuiText>

            <EuiSpacer size="l" />
                <EuiFieldText
                  placeholder="Chemin du fichier dans le serveur"
                  value={this.state.path_name}
                  onChange={e => {
                    this.on_change_path(e);
                  }}
                />
                <EuiSelect
                  id="select type of file"
                  options={this.state.options}
                  value={this.state.field_name}
                  onChange={e => {
                    this.on_change_field(e);
                  }}
                />
                <EuiFieldText
                  placeholder="Supplément de nom pour l'index"
                  value={this.state.index_name}
                  onChange={e => {
                    this.on_change_name(e);
                  }}
                />
                <EuiSpacer size="m" />
                <EuiButton
                  onClick={() => this.handle_click()}>
                  Envoyer les données
                </EuiButton>
                <EuiSpacer size="m" />
                <FormattedMessage
                    id ="infos"
                    defaultMessage="{error}"
                    values={{ error: this.state.error }}
                />
                <EuiSpacer size="l" />
                <EuiText>
                <h1>Mise à jour des données</h1>
                </EuiText>
                <EuiSpacer size="l" />
                <IndexesTable
                  ref={this.myRef}
                  items={this.state.indexes}
                  parent={this}
                />
                <EuiSpacer size="l" />
                <EuiText>
                <h1>Mise à jour du modèle</h1>
                <p>La création du modèle est une tâche lourde qu'il n'est pas nécessaire de réaliser plus d'une fois tous les ans.
                </p>
                </EuiText>
                <EuiSpacer size="l" />
                <div>
                    <EuiFlexGrid columns={2}>
                      <EuiFlexItem>
                        <div style={{"fontWeight": "bold"}}>Corporel</div>
                      </EuiFlexItem>
                      <EuiFlexItem>
                        <div style={{"fontWeight": "bold"}}>Matériel</div>
                      </EuiFlexItem>
                      <EuiFlexItem>
                        <div>Précision : {corporel_precision}</div>
                      </EuiFlexItem>
                      <EuiFlexItem>
                        <div>Précision : {materiel_precision}</div>
                      </EuiFlexItem>
                      <EuiFlexItem>
                        <div>Recall : {corporel_recall}</div>
                      </EuiFlexItem>
                      <EuiFlexItem>
                        <div>Recall : {materiel_recall}</div>
                      </EuiFlexItem>
                      <EuiFlexItem>
                        <div>Coefficient de Giny : {corporel_giny}</div>
                      </EuiFlexItem>
                      <EuiFlexItem>
                        <div>Coefficient de Giny : {materiel_giny}</div>
                      </EuiFlexItem>
                      <EuiFlexItem>
                        <div style={{"fontWeight": "bold"}}>Matrice de confusion</div>
                      </EuiFlexItem>
                      <EuiFlexItem>
                        <div style={{"fontWeight": "bold"}}>Matrice de confusion</div>
                      </EuiFlexItem>
                      <EuiFlexItem>
                        <table style={style_table}>
                        <tbody>
                          <tr>
                            <td style={{
                              "border": "1px solid black", "backgroundColor":"green", 
                              "fontWeight":"bold", "color":"white", "textAlign":"center",
                              "verticalAlign":"middle"
                          }}>
                              {corporel_confusion_0_0}
                            </td>
                            <td style={{
                              "border": "1px solid black", "backgroundColor":"red", 
                              "fontWeight":"bold", "color":"white", "textAlign":"center",
                              "verticalAlign":"middle"
                          }}>
                              {corporel_confusion_0_1}
                            </td>
                          </tr>
                          <tr>
                            <td style={{
                              "border": "1px solid black", "backgroundColor":"orange", 
                              "fontWeight":"bold", "color":"white", "textAlign":"center",
                              "verticalAlign":"middle"
                          }}>
                              {corporel_confusion_1_0}
                            </td>
                            <td style={{
                              "border": "1px solid black", "backgroundColor":"green", 
                              "fontWeight":"bold", "color":"white", "textAlign":"center",
                              "verticalAlign":"middle"
                          }}>
                              {corporel_confusion_1_1}
                            </td>
                          </tr>
                          </tbody>
                        </table>
                      </EuiFlexItem>
                      <EuiFlexItem>
                        <table style={style_table}>
                        <tbody>
                          <tr>
                            <td style={{
                              "border": "1px solid black", "backgroundColor":"green", 
                              "fontWeight":"bold", "color":"white", "textAlign":"center",
                              "verticalAlign":"middle"
                          }}>
                              {materiel_confusion_0_0}
                            </td>
                            <td style={{
                              "border": "1px solid black", "backgroundColor":"red", 
                              "fontWeight":"bold", "color":"white", "textAlign":"center",
                              "verticalAlign":"middle"
                          }}>
                              {materiel_confusion_0_1}
                            </td>
                          </tr>
                          <tr>
                            <td style={{
                              "border": "1px solid black", "backgroundColor":"orange", 
                              "fontWeight":"bold", "color":"white", "textAlign":"center",
                              "verticalAlign":"middle"
                          }}>
                              {materiel_confusion_1_0}
                            </td>
                            <td style={{
                              "border": "1px solid black", "backgroundColor":"green", 
                              "fontWeight":"bold", "color":"white", "textAlign":"center",
                              "verticalAlign":"middle"
                          }}>
                              {materiel_confusion_1_1}
                            </td>
                          </tr>
                          </tbody>
                        </table>
                      </EuiFlexItem>
                    </EuiFlexGrid>
                  </div>
                  <EuiSpacer size="xxl" />
                <EuiText>
                  <span style={{"fontWeight": "bold"}}>Date du modèle : </span><span>{date_training}</span>
                </EuiText>
                <EuiText>
                  <span style={{"fontWeight": "bold"}}>Statut du modèle : </span><span>{message_training}</span>
                </EuiText>
                <EuiSpacer size="m" />
                <EuiButton
                  onClick={() => this.handle_training()}>
                  Entraîner le modèle
                </EuiButton>
                <EuiSpacer size="m" />
                <EuiText>
                  <p>{error_training}</p>
                </EuiText>
            </EuiPageContentBody>
          </EuiPageContent>
        </EuiPageBody>
      </EuiPage>
    );
  }
}
