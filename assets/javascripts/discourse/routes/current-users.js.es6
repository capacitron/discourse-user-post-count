export default Discourse.Route.extend({
  queryParams: {
    current_period: { refreshModel: true },
    order: { refreshModel: true },
    asc: { refreshModel: true },
    name: { refreshModel: true, replace: true }
  },

  refreshQueryWithoutTransition: true,

  titleToken() {
    return I18n.t("current_directory.title");
  },

  resetController(controller, isExiting) {
    if (isExiting) {
      controller.setProperties({
        current_period: "first_quarterly",
        order: "total_participation",
        asc: null,
        name: ""
      });
    }
  },

  beforeModel() {
    if (this.siteSettings.hide_user_profiles_from_public && !this.currentUser) {
      this.replaceWith("discovery");
    }
  },

  model(params) {
    // If we refresh via `refreshModel` set the old model to loading
    this._params = params;
    return this.store.find("currentDirectoryItem", params);
  },

  setupController(controller, model) {
    const params = this._params;
    controller.setProperties({ model, current_period: params.current_period, nameInput: params.name });
  },

  actions: {
    didTransition() {
      this.controllerFor("current-users")._showFooter();
      return true;
    }
  }
});
