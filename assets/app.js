var IndividualResults = function({results}) {
    const items = results.map((row) =>
        <tr>
            <td>{row.Value}</td>
            <td>{row.Which}</td>
            <td>{row.Count}</td>
            <td>{row.First}</td>
            <td>{row.Last}</td>
        </tr>
    );
    return (
        <table width="100%">
            <thead>
                <tr>
                    <th>Value</th>
                    <th>Which</th>
                    <th>Count</th>
                    <th>First</th>
                    <th>Last</th>
                </tr>
            </thead>
            <tbody>
                {items}
            </tbody>
        </table>
    )
}
var TupleResults = function({results}) {
    const items = results.map((row) =>
        <tr>
            <td>{row.Query}</td>
            <td>{row.Type}</td>
            <td>{row.Answer}</td>
            <td>{row.Count}</td>
            <td>{row.TTL}</td>
            <td>{row.First}</td>
            <td>{row.Last}</td>
        </tr>
    );
    return (
        <table width="100%" border="1">
            <thead>
                <tr>
                    <th>Query</th>
                    <th>Type</th>
                    <th>Answer</th>
                    <th>Count</th>
                    <th>TTL</th>
                    <th>First</th>
                    <th>Last</th>
                </tr>
            </thead>
            <tbody>
                {items}
            </tbody>
        </table>
    )
}

class Searcher extends React.Component {
    constructor(props) {
        super(props);
        this.state = {msg: "", results: null};
    }

    componentWillReceiveProps(nextProps) {
        if(nextProps.query !== this.props.query)
            this.doSearch(nextProps.query)
    }
    componentDidMount() {
        this.doSearch(this.props.query)
    }

    doSearch(query) {
        if(!query)
            return;

        this.setState({msg: "Searching..."});
        fetch(`/dns/like/${this.props.kind}/${query}`).then((resp) => {
            return resp.json();
        }).then((data) => {
            console.log(data.length, "records");
            this.setState({msg: "", results: data});
        });
    }

    render() {
        if(!this.props.query) {
            return (null);
        }
        if(this.state.results === null) {
            return <div>{this.state.msg}</div>;
        }
        if(this.props.kind === "tuples") 
            return (
                <div>
                    <h2>Tuples</h2>
                    <TupleResults results={this.state.results}/>
                </div>
            )
        else 
            return (
                <div>
                    <h2>Individual</h2>
                    <IndividualResults results={this.state.results}/>
                </div>
            )
    }
}

class SearchForm extends React.Component {
    constructor(props) {
        super(props);
        this.handleKeyPress = this.handleKeyPress.bind(this);
    }

    handleKeyPress(e) {
        if (e.key === 'Enter') {
            this.props.onEnter()
        }
    }

    render() {
        return (
            <label>Query
                <input type="text" name="q" value={this.props.query} onChange={this.props.onChange} onKeyPress={this.handleKeyPress}/>
            </label>
        )
    }
}

class SearchComponent extends React.Component {
    constructor(props) {
        super(props);
        this.state = {query: "", activequery: null};
        this.queryChanged = this.queryChanged.bind(this);
        this.runQuery = this.runQuery.bind(this);
    }

    queryChanged(event) {
        this.setState({query: event.target.value});
    }
    runQuery() {
        this.setState({activequery: this.state.query});
    }

    render() {
        return (
            <div>
                <h1>PDNS!</h1>
                <SearchForm query={this.state.query} onChange={this.queryChanged} onEnter={this.runQuery}/>
                <Searcher kind="individual" query={this.state.activequery}/>
                <Searcher kind="tuples" query={this.state.activequery}/>
            </div>
        );
    }
};

ReactDOM.render(<SearchComponent />, document.getElementById('app'));
