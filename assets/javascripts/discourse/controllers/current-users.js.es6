import debounce from 'discourse/lib/debounce';

export default Ember.Controller.extend({
  needs: ["application"],
  queryParams: ["current_period", "order", "asc", "name"],
  current_period: current_quarter_of_the_year(),
  order: "total_participation",
  asc: null,
  name: "",

  showTimeRead: Ember.computed.equal("current_period", "all"),

  _setName: debounce(function() {
    this.set("name", this.get("nameInput"));
  }, 500).observes("nameInput"),

  _showFooter: function() {
    this.set("controllers.application.showFooter", !this.get("model.canLoadMore"));
  }.observes("model.canLoadMore"),

  actions: {
    loadMore() {
      this.get("model").loadMore();
    }
  }
});

function current_quarter_of_the_year() {
  var current_quarter;
  var month = (new Date()).getMonth() + 1;
  var quarter_number = (Math.ceil(month / 3));
  switch (quarter_number) {
    case 1:
      current_quarter = "first_quarterly";
      break;
    case 2:
      current_quarter = "second_quarterly";
      break;
    case 3:
      current_quarter = "third_quarterly";
      break;
    case 4:
      current_quarter = "fourth_quarterly";
      break;
  }
  return current_quarter;
}
