import React, { Component } from 'react';


import {
  EuiBasicTable,
  EuiCode,
  EuiLink,
  EuiHealth,
  EuiFlexGroup,
  EuiFlexItem,
  EuiSpacer,
  EuiSwitch,
  EuiText,
  EuiButton,
} from '@elastic/eui';


export class IndexesTable extends Component {
  constructor(props) {
    super(props);
    this.state = {
      pageIndex: 0,
      pageSize: 10,
      showPerPageOptions: true,
      totalItemCount:props.items.length,
      ready:false,  
      sortField: 'date',
      sortDirection: 'asc',
    };
  }

  onTableChange = ({ page = {}, sort = {} }) => {
    const { index: pageIndex, size: pageSize } = page;
    const { field: sortField, direction: sortDirection } = sort;
    console.log(pageIndex, pageSize, sortField, sortDirection);
    this.setState({
      pageIndex:pageIndex,
      pageSize:pageSize,
      sortField:sortField,
      sortDirection:sortDirection
    });
  };

  renderStatus = online => {
    const color = online ? 'success' : 'danger';
    const label = online ? 'Oui' : 'Non';
    return <EuiHealth color={color}>{label}</EuiHealth>;
  };

  togglePerPageOptions = () =>
    this.setState(state => ({ showPerPageOptions: !state.showPerPageOptions }));

  harmonisate = name => {
    return <EuiButton
                  onClick={() => this.props.parent.handle_harmonisation.bind(this.props.parent)(name)}>
                  Harmoniser
                </EuiButton>
  }
  render() {
    console.log("actualise");
    const { pageIndex, pageSize, showPerPageOptions, totalItemCount, sortField, sortDirection} = this.state;

    function GetSortOrder(field, direction) {    
      return function(a, b) {    
        if (a[field] > b[field]) {
            if (direction == "asc"){
              return 1; 
            }
            else{
              return -1;
            }
        } else if (a[field] < b[field]) {    
            if (direction == "asc"){
              return -1; 
            }
            else{
              return 1;
            }   
        }    
        return 0;    
      };    
    }
    
    this.props.items.sort(GetSortOrder(sortField, sortDirection));

    var items = this.props.items.slice(pageIndex*pageSize, (pageIndex+1)*pageSize);


    const columns = [
      {
        field: 'name',
        name: "Nom de l'index",
        truncateText: true,
        sortable: true,
        render: text => (<EuiText>{text}</EuiText>)
      },
      {
        field: 'date',
        name: 'Date',
        truncateText: true,
        sortable: true,
        dataType: 'date',
        render: text => (<EuiText>{text}</EuiText>)
      },
      {
        field: 'category',
        name: "Type de l'index",
        sortable: true,
        render: text => (<EuiText>{text}</EuiText>)
      },
      {
        field: 'harmonise',
        name: 'Harmonisé ?',
        dataType: 'boolean',
        sortable: true,
        render: harmonise => this.renderStatus(harmonise),
      },
      {
        field: "name",
        name: "Bouton d'harmonisation",
        render: name => this.harmonisate(name)
      }
    ];

    const pagination = {
      pageIndex,
      pageSize,
      totalItemCount,
      hidePerPageOptions: !showPerPageOptions,
    };

    const sorting = {
      sort: {
        field: sortField,
        direction: sortDirection,
      },
    };
    return (
      <div>
        <EuiBasicTable
          items={items}
          columns={columns}
          pagination={pagination}
          sorting={sorting}
          onChange={this.onTableChange}
        />
      </div>
    );
  }
}