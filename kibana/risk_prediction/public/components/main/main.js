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
  EuiForm,
  EuiFormRow,
  EuiRange,
  EuiButton,
  EuiLoadingSpinner,
  EuiSwitch,
  EuiFlexItem,
  EuiFlexGroup,
  EuiProgress,
  EuiDatePicker,
  EuiSelect,
  EuiSpacer,
} from '@elastic/eui';

import moment from 'moment';

export class Main extends React.Component {
  constructor(props) {
    super(props);
    var options = [
                  { value: 'Pic_Matin', text: 'Pic du matin' },
                  { value: 'Journee', text: 'Journée' },
                  { value: 'Pic_Soir', text: 'Pic du soir' },
                  { value: 'Nuit', text: 'Nuit' }
                ]
    this.state = {
    httpClient:this.props.httpClient,
    temperature: 15,
    pluviometrie1: 0,
    pluviometrie6: 0,
    pression: 1023,
    direction: 0,
    vitesse: 0,
    date:moment('2020-07-04'),
    creneau:"Journee",
    options:options
    };
  
  }

  componentDidMount() {
  }
  
  
  onTemperatureChange = e => {
    this.setState({
      temperature: e.target.value,
    
    });
  };
  
  onPluviometrie1Change = e => {
    this.setState({
      pluviometrie1: e.target.value,
    });
  };
  
  onPluviometrie6Change = e => {
    this.setState({
      pluviometrie6: e.target.value,
    });
  };

  onPressionChange = e => {
    this.setState({
      pression: e.target.value,
    });
  };

  onDirectionChange = e => {
    this.setState({
      direction: e.target.value,
    });
  };

  onVitesseChange = e => {
    this.setState({
      vitesse: e.target.value,
    });
  };
  onVitesseChange = e => {
    this.setState({
      vitesse: e.target.value,
    });
  };
  onCreneauChange = e => {
    this.setState({
      creneau: e.target.value,
    });
  };
  onDateChange = date => {
    this.setState({"date":date});
  };

  handle_prediction(){

    this.state.httpClient.post('../api/data_management/to_python', 
        { 
          "path_python":"prediction",
            "Temperature": this.state.temperature,
            "Pluviometrie 1h": this.state.pluviometrie1,
            "Pluviometrie 6h": this.state.pluviometrie6,
            "Pression 0m": this.state.pression,
            "Direction Vent": this.state.direction,
            "Vitesse Vent": this.state.vitesse,
            "Date":this.state.date,
            "Creneau_Horaire":this.state.creneau,
        },
        ["Content-Type: application/x-ndjson"]
      ).then((resp) => {
	      console.log(resp);
              },
        function(error){
		console.log(error);
    });
  }

  
  render() {
    var date = this.state.date;
    const { title } = this.props;
    return (
      <EuiPage>
        <EuiPageBody>
          <EuiPageHeader>
            <EuiTitle size="l">
              <h1>
                Modèle prédictif
              </h1>
            </EuiTitle>
          </EuiPageHeader>
          <EuiPageContent>
            <EuiPageContentHeader>
            </EuiPageContentHeader>
            <EuiPageContentBody>
    <EuiText>
        <h2>
          Prédiction
        </h2>
        <EuiSpacer size="m" />
        
        Une fois le modèle créer, il devient possible de prédire l'accidentologie sur les routes du Loiret.<br/>
        Choisissez une date, un créneau horaire et des données météo puis lancez une prédiction.
        <EuiSpacer size="l" />
        </EuiText>
              <EuiForm>
        <EuiFormRow label="Selectionner le jour à prédire">
          <EuiDatePicker  dateFormat='DD/MM/YYYY' selected={date} onChange={this.onDateChange} />
        </EuiFormRow>
        <EuiFormRow label="Selectionner le créneau horaire">
          <EuiSelect
                  id="creneau_select"
                  options={this.state.options}
                  value={this.state.creneau}
                  onChange={e => {
                    this.onCreneauChange(e);
                  }}
                />
        </EuiFormRow>
        <EuiFormRow label="Température (degré celsius)" helpText="Température prévue">
          <EuiRange
            min={-20}
            max={50}
            step={0.1}
            value={this.state.temperature}
            onChange={this.onTemperatureChange}
            showInput
            showLabels
          />
          </EuiFormRow>
        <EuiFormRow label="Pluviométrie à 1h (mm)" helpText="Pluviométrie prévue à 1h">
          <EuiRange
            min={0}
            max={1000}
            step={0.1}
            value={this.state.pluviometrie1}
            onChange={this.onPluviometrie1Change}
            showLabels
            showInput
          />
          </EuiFormRow>
        <EuiFormRow label="Pluviométrie à 6h (mm)" helpText="Pluviométrie prévue à 6h">
          <EuiRange
            min={0}
            max={3000}
            step={0.1}
            value={this.state.pluviometrie6}
            onChange={this.onPluviometrie6Change}
            showLabels
            showInput
          />
          </EuiFormRow>
        <EuiFormRow label="Pression au niveau de la mer (hectopascal)" helpText="Pression atmosphérique prévue au niveau de la mer">
          <EuiRange
            min={1000}
            max={1050}
            step={0.1}
            value={this.state.pression}
            onChange={this.onPressionChange}
            showLabels
            showInput
          />
          </EuiFormRow>
        <EuiFormRow label="Direction du vent (degré)" helpText="Direction prévue du vent">
          <EuiRange
            min={0}
            max={360}
            step={1}
            value={this.state.direction}
            onChange={this.onDirectionChange}
            showLabels
            showInput
          />
          </EuiFormRow>
        <EuiFormRow label="Vitesse du vent (km/h)" helpText="Vitesse prévue du vent">
          <EuiRange
            min={0}
            max={300}
            step={1}
            value={this.state.vitesse}
            onChange={this.onVitesseChange}
            showLabels
            showInput
          />
        </EuiFormRow>

        <EuiButton type="submit" onClick={() => this.handle_prediction()}>
          Prévision du risque d'accidents
        </EuiButton>
        </EuiForm>

            </EuiPageContentBody>
          </EuiPageContent>
        </EuiPageBody>
      </EuiPage>
    );
  }
}
